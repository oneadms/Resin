package routing

import (
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

var ErrNoAvailableNodes = errors.New("no available nodes")

var randomRouteRNGPool = sync.Pool{
	New: func() any {
		return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	},
}

const (
	randomRouteMaxCandidates = 4
	randomRouteFullScanLimit = 32
	balancedLeasePenalty     = 0.15
)

// randomRoute selects a routable node using a small random candidate set with
// latency/bandwidth/load scoring. Small platform views are fully scanned so
// compact AI pools consistently pick the best measured node.
// It intentionally trusts Platform.View as the routable source of truth. For
// larger views it caps quality checks to a few random candidates; post-pick
// race handling (node removed right after selection) is handled by the caller
// in RouteRequest.
func randomRoute(
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
	targetDomain string,
	authorities []string,
	p2cWindow time.Duration,
) (node.Hash, error) {
	view := plat.View()
	size := view.Size()
	if size == 0 {
		return node.Zero, ErrNoAvailableNodes
	}

	rng := randomRouteRNGPool.Get().(*rand.Rand)
	defer randomRouteRNGPool.Put(rng)

	pick := func() (node.Hash, bool) {
		return view.RandomPick(rng)
	}

	first, ok := pick()
	if !ok {
		return node.Zero, ErrNoAvailableNodes
	}

	// If view has one node, use it directly.
	if size == 1 {
		return first, nil
	}

	now := time.Now()
	bestHash := first
	bestScore := math.MaxFloat64
	seen := map[node.Hash]struct{}{first: {}}
	sampleLimit := min(size, randomRouteMaxCandidates)

	scoreCandidate := func(h node.Hash) {
		entry, ok := pool.GetEntry(h)
		score := math.MaxFloat64
		if ok {
			cost := entryPerformanceCostMs(entry, targetDomain, authorities, now, p2cWindow)
			score = calculateScore(entry, cost, plat, stats)
		}
		// Favor the later candidate on exact ties to retain a little randomness
		// when all quality/load inputs are equal.
		if score <= bestScore {
			bestHash = h
			bestScore = score
		}
	}
	scoreCandidate(first)

	if size <= randomRouteFullScanLimit {
		view.Range(func(h node.Hash) bool {
			if _, exists := seen[h]; exists {
				return true
			}
			seen[h] = struct{}{}
			scoreCandidate(h)
			return true
		})
		return bestHash, nil
	}

	for attempts := 0; len(seen) < sampleLimit && attempts < sampleLimit*4; attempts++ {
		candidate, ok := pick()
		if !ok {
			break
		}
		if _, exists := seen[candidate]; exists {
			continue
		}
		seen[candidate] = struct{}{}
		scoreCandidate(candidate)
	}

	return bestHash, nil
}

// compareLatencies determines the latency values for h1 and h2.
// Implements the 3-level comparison logic:
// 1. Target domain present in both and recent.
// 2. Common authority domains present in both and recent.
// 3. Fallback to 0 (empty) for both.
func compareLatencies(
	h1, h2 node.Hash,
	pool PoolAccessor,
	target string,
	authorities []string,
	window time.Duration,
) (time.Duration, time.Duration) {
	e1, ok1 := pool.GetEntry(h1)
	e2, ok2 := pool.GetEntry(h2)
	if !ok1 || !ok2 || e1.LatencyTable == nil || e2.LatencyTable == nil {
		return 0, 0
	}

	now := time.Now()
	return compareEntryLatencies(e1, e2, target, authorities, now, window)
}

func compareEntryLatencies(
	e1, e2 *node.NodeEntry,
	target string,
	authorities []string,
	now time.Time,
	window time.Duration,
) (time.Duration, time.Duration) {
	if e1 == nil || e2 == nil || e1.LatencyTable == nil || e2.LatencyTable == nil {
		return 0, 0
	}
	// 1. Target domain check.
	// target can be empty if extracted domain is invalid/empty, handle gracefully.
	lat1, ok1 := lookupRecentDomainLatency(e1, target, now, window)
	lat2, ok2 := lookupRecentDomainLatency(e2, target, now, window)
	if ok1 && ok2 {
		return lat1, lat2
	}

	// 2. Authority intersection check.
	lat1, lat2, ok := averageComparableAuthorityLatencies(e1, e2, authorities, now, window)
	if ok {
		return lat1, lat2
	}

	// 3. Fallback.
	return 0, 0
}

func isRecent(t time.Time, now time.Time, window time.Duration) bool {
	return now.Sub(t) <= window
}

// calculateScore computes the score for a node based on platform allocation policy.
// Lower is better.
func calculateScore(
	entry *node.NodeEntry,
	performanceCost float64,
	plat *platform.Platform,
	stats *IPLoadStats,
) float64 {
	// Lease count from stats.
	var leaseCount int64
	if entry != nil {
		ip := entry.GetEgressIP()
		if ip.IsValid() {
			leaseCount = stats.Get(ip)
		}
	}

	// If no comparable performance data exists, score by lease count.
	if performanceCost <= 0 {
		return float64(leaseCount)
	}

	// Policy-based scoring.
	switch plat.AllocationPolicy {
	case platform.AllocationPolicyPreferLowLatency:
		return performanceCost
	case platform.AllocationPolicyPreferIdleIP:
		return float64(leaseCount)
	case platform.AllocationPolicyBalanced:
		fallthrough
	default:
		return performanceCost * (1 + float64(leaseCount)*balancedLeasePenalty)
	}
}

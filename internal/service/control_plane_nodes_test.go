package service

import (
	"errors"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func newNodeListTestPool(subMgr *topology.SubscriptionManager) *topology.GlobalNodePool {
	return topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})
}

func addRoutableNodeForSubscription(
	t *testing.T,
	pool *topology.GlobalNodePool,
	sub *subscription.Subscription,
	raw []byte,
	egressIP string,
) node.Hash {
	return addRoutableNodeForSubscriptionWithTag(t, pool, sub, raw, egressIP, "tag")
}

func addRoutableNodeForSubscriptionWithTag(
	t *testing.T,
	pool *topology.GlobalNodePool,
	sub *subscription.Subscription,
	raw []byte,
	egressIP string,
	tag string,
) node.Hash {
	t.Helper()

	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{tag}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s not found after add", hash.Hex())
	}
	entry.SetEgressIP(netip.MustParseAddr(egressIP))
	if entry.LatencyTable == nil {
		t.Fatalf("node %s latency table not initialized", hash.Hex())
	}
	entry.LatencyTable.Update("example.com", 25*time.Millisecond, 10*time.Minute)
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	pool.RecordResult(hash, true)
	pool.NotifyNodeDirty(hash)
	return hash
}

func TestListNodes_PlatformAndSubscriptionFiltersReturnIntersection(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	plat := platform.NewPlatform("plat-1", "plat", nil, nil)
	pool.RegisterPlatform(plat)

	subA := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subB := subscription.NewSubscription("sub-b", "sub-b", "https://example.com/b", true, false)
	subMgr.Register(subA)
	subMgr.Register(subB)

	hashA := addRoutableNodeForSubscription(
		t,
		pool,
		subA,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.10",
	)
	_ = addRoutableNodeForSubscription(
		t,
		pool,
		subB,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.11",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}
	filters := NodeFilters{
		PlatformID:     &plat.ID,
		SubscriptionID: &subA.ID,
	}

	nodes, err := cp.ListNodes(filters)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("intersection size = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != hashA.Hex() {
		t.Fatalf("intersection node hash = %q, want %q", nodes[0].NodeHash, hashA.Hex())
	}
}

func TestListNodes_SubscriptionFilterSkipsStaleManagedNodes(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	staleHash := node.HashFromRawOptions([]byte(`{"type":"ss","server":"9.9.9.9","port":443}`))
	sub.ManagedNodes().StoreNode(staleHash, subscription.ManagedNode{Tags: []string{"stale"}})

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}
	filters := NodeFilters{
		SubscriptionID: &sub.ID,
	}

	nodes, err := cp.ListNodes(filters)
	if err != nil {
		t.Fatalf("ListNodes with stale hash: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("nodes with stale managed hash = %d, want 0", len(nodes))
	}

	liveHash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.20",
	)

	nodes, err = cp.ListNodes(filters)
	if err != nil {
		t.Fatalf("ListNodes with stale+live hashes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("nodes with stale+live hashes = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != liveHash.Hex() {
		t.Fatalf("live node hash = %q, want %q", nodes[0].NodeHash, liveHash.Hex())
	}
}

func TestListNodes_SubscriptionFilterSkipsEvictedManagedNodes(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	subA := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subB := subscription.NewSubscription("sub-b", "sub-b", "https://example.com/b", true, false)
	subMgr.Register(subA)
	subMgr.Register(subB)

	raw := []byte(`{"type":"ss","server":"7.7.7.7","port":443}`)
	hash := addRoutableNodeForSubscriptionWithTag(t, pool, subA, raw, "203.0.113.40", "a-tag")
	pool.AddNodeFromSub(hash, raw, subB.ID)
	subB.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"b-tag"}})

	managedA, ok := subA.ManagedNodes().LoadNode(hash)
	if !ok {
		t.Fatal("subA managed node missing before eviction")
	}
	managedA.Evicted = true
	subA.ManagedNodes().StoreNode(hash, managedA)
	pool.RemoveNodeFromSub(hash, subA.ID)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	filtersA := NodeFilters{SubscriptionID: &subA.ID}
	nodesA, err := cp.ListNodes(filtersA)
	if err != nil {
		t.Fatalf("ListNodes subA: %v", err)
	}
	if len(nodesA) != 0 {
		t.Fatalf("subA filtered nodes = %d, want 0", len(nodesA))
	}

	filtersB := NodeFilters{SubscriptionID: &subB.ID}
	nodesB, err := cp.ListNodes(filtersB)
	if err != nil {
		t.Fatalf("ListNodes subB: %v", err)
	}
	if len(nodesB) != 1 || nodesB[0].NodeHash != hash.Hex() {
		t.Fatalf("subB filtered nodes = %+v, want [%s]", nodesB, hash.Hex())
	}
}

func TestGetNode_TagIncludesSubscriptionNamePrefix(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	got, err := cp.GetNode(hash.Hex())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if len(got.Tags) != 1 {
		t.Fatalf("tags len = %d, want 1", len(got.Tags))
	}
	if got.Tags[0].Tag != "sub-a/tag" {
		t.Fatalf("tag = %q, want %q", got.Tags[0].Tag, "sub-a/tag")
	}
}

func TestGetNode_ReferenceLatencyMsUsesAuthorityAverage(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing", hash.Hex())
	}
	entry.LatencyTable.LoadEntry("cloudflare.com", node.DomainLatencyStats{
		Ewma:        40 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.LatencyTable.LoadEntry("github.com", node.DomainLatencyStats{
		Ewma:        60 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        5 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	runtimeCfg := &atomic.Pointer[config.RuntimeConfig]{}
	cfg := config.NewDefaultRuntimeConfig()
	cfg.LatencyAuthorities = []string{"cloudflare.com", "github.com", "google.com"}
	runtimeCfg.Store(cfg)

	cp := &ControlPlaneService{
		Pool:       pool,
		SubMgr:     subMgr,
		GeoIP:      &geoip.Service{},
		RuntimeCfg: runtimeCfg,
	}

	got, err := cp.GetNode(hash.Hex())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.ReferenceLatencyMs == nil {
		t.Fatal("reference_latency_ms should be present")
	}
	if *got.ReferenceLatencyMs != 50 {
		t.Fatalf("reference_latency_ms = %v, want 50", *got.ReferenceLatencyMs)
	}
}

func TestGetNode_ManualDisabledAffectsSummaryAndEnabledFilter(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)
	pool.SetNodeManualDisabled(hash, true)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	got, err := cp.GetNode(hash.Hex())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.ManualDisabled {
		t.Fatal("manual_disabled should be true")
	}
	if got.Enabled {
		t.Fatal("enabled should be false when manually disabled")
	}

	enabled := true
	nodes, err := cp.ListNodes(NodeFilters{Enabled: &enabled})
	if err != nil {
		t.Fatalf("ListNodes(enabled=true): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("enabled=true nodes len = %d, want 0", len(nodes))
	}

	enabled = false
	nodes, err = cp.ListNodes(NodeFilters{Enabled: &enabled})
	if err != nil {
		t.Fatalf("ListNodes(enabled=false): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != hash.Hex() {
		t.Fatalf("enabled=false nodes = %+v, want %s", nodes, hash.Hex())
	}
}

func TestUpdateNode_ManualDisabled(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	got, err := cp.UpdateNode(hash.Hex(), []byte(`{"manual_disabled":true}`))
	if err != nil {
		t.Fatalf("UpdateNode(disable): %v", err)
	}
	if !got.ManualDisabled || got.Enabled {
		t.Fatalf("disabled summary = %+v, want manual_disabled=true enabled=false", got)
	}
	if !pool.IsNodeDisabled(hash) {
		t.Fatal("pool should report manually disabled node as disabled")
	}

	got, err = cp.UpdateNode(hash.Hex(), []byte(`{"manual_disabled":false}`))
	if err != nil {
		t.Fatalf("UpdateNode(enable): %v", err)
	}
	if got.ManualDisabled || !got.Enabled {
		t.Fatalf("enabled summary = %+v, want manual_disabled=false enabled=true", got)
	}
}

func TestProbeLatencyBatch_DisablesOnlyNodesAboveThreshold(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	fastHash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)
	slowHash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.31",
	)
	failedHash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"3.3.3.3","port":443}`),
		"203.0.113.32",
	)

	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: pool,
		Fetcher: func(hash node.Hash, _ string) ([]byte, time.Duration, error) {
			switch hash {
			case fastHash:
				return []byte("ok"), 80 * time.Millisecond, nil
			case slowHash:
				return []byte("ok"), 1500 * time.Millisecond, nil
			case failedHash:
				return nil, 0, errors.New("probe failed")
			default:
				return nil, 0, errors.New("unexpected node")
			}
		},
	})

	cp := &ControlPlaneService{
		Pool:     pool,
		SubMgr:   subMgr,
		GeoIP:    &geoip.Service{},
		ProbeMgr: probeMgr,
	}

	result, err := cp.ProbeLatencyBatch(NodeFilters{SubscriptionID: &sub.ID}, 1000)
	if err != nil {
		t.Fatalf("ProbeLatencyBatch: %v", err)
	}
	if result.MatchedCount != 3 || result.TestedCount != 2 || result.DisabledCount != 1 || result.FailedCount != 1 {
		t.Fatalf("batch result = %+v", result)
	}
	if pool.IsNodeDisabled(fastHash) {
		t.Fatal("fast node should remain enabled")
	}
	if !pool.IsNodeDisabled(slowHash) {
		t.Fatal("slow node should be manually disabled")
	}
	if pool.IsNodeDisabled(failedHash) {
		t.Fatal("failed probe node should not be manually disabled")
	}
}

func TestProbeBandwidthBatch_DisablesOnlyNodesBelowThreshold(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	fastHash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)
	slowHash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.31",
	)
	failedHash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"3.3.3.3","port":443}`),
		"203.0.113.32",
	)

	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: pool,
		BandwidthFetcher: func(hash node.Hash, _ string, _ int64) (int64, time.Duration, error) {
			switch hash {
			case fastHash:
				return 5_000_000, 2 * time.Second, nil
			case slowHash:
				return 5_000_000, 10 * time.Second, nil
			case failedHash:
				return 0, 0, errors.New("download failed")
			default:
				return 0, 0, errors.New("unexpected node")
			}
		},
		UploadFetcher: func(hash node.Hash, _ string, _ int64) (int64, time.Duration, error) {
			switch hash {
			case fastHash, slowHash:
				return 2_000_000, time.Second, nil
			case failedHash:
				return 0, 0, errors.New("upload failed")
			default:
				return 0, 0, errors.New("unexpected node")
			}
		},
	})

	cp := &ControlPlaneService{
		Pool:     pool,
		SubMgr:   subMgr,
		GeoIP:    &geoip.Service{},
		ProbeMgr: probeMgr,
	}

	result, err := cp.ProbeBandwidthBatch(NodeFilters{SubscriptionID: &sub.ID}, 10, 5)
	if err != nil {
		t.Fatalf("ProbeBandwidthBatch: %v", err)
	}
	if result.MatchedCount != 3 || result.TestedCount != 2 || result.DisabledCount != 1 || result.FailedCount != 1 {
		t.Fatalf("batch result = %+v", result)
	}
	if pool.IsNodeDisabled(fastHash) {
		t.Fatal("fast node should remain enabled")
	}
	if !pool.IsNodeDisabled(slowHash) {
		t.Fatal("slow node should be manually disabled")
	}
	if pool.IsNodeDisabled(failedHash) {
		t.Fatal("failed probe node should not be manually disabled")
	}
}

func TestProbeQualityBatch_UsesBothThresholdsAndKeepsFailures(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("sub-quality", "sub-quality", "https://example.com/quality", true, false)
	subMgr.Register(sub)

	goodHash := addRoutableNodeForSubscription(
		t, pool, sub, []byte(`{"id":"quality-good"}`), "203.0.113.40",
	)
	highLatencyHash := addRoutableNodeForSubscription(
		t, pool, sub, []byte(`{"id":"quality-latency"}`), "203.0.113.41",
	)
	lowBandwidthHash := addRoutableNodeForSubscription(
		t, pool, sub, []byte(`{"id":"quality-bandwidth"}`), "203.0.113.42",
	)
	failedHash := addRoutableNodeForSubscription(
		t, pool, sub, []byte(`{"id":"quality-failed"}`), "203.0.113.43",
	)

	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: pool,
		Fetcher: func(hash node.Hash, _ string) ([]byte, time.Duration, error) {
			switch hash {
			case goodHash, lowBandwidthHash:
				return []byte("ok"), 80 * time.Millisecond, nil
			case highLatencyHash:
				return []byte("ok"), 1500 * time.Millisecond, nil
			case failedHash:
				return nil, 0, errors.New("latency failed")
			default:
				return nil, 0, errors.New("unexpected node")
			}
		},
		BandwidthFetcher: func(hash node.Hash, _ string, _ int64) (int64, time.Duration, error) {
			switch hash {
			case goodHash, highLatencyHash:
				return 5_000_000, 2 * time.Second, nil
			case lowBandwidthHash:
				return 5_000_000, 10 * time.Second, nil
			case failedHash:
				return 0, 0, errors.New("bandwidth failed")
			default:
				return 0, 0, errors.New("unexpected node")
			}
		},
		UploadFetcher: func(hash node.Hash, _ string, _ int64) (int64, time.Duration, error) {
			switch hash {
			case goodHash, highLatencyHash, lowBandwidthHash:
				return 2_000_000, time.Second, nil
			case failedHash:
				return 0, 0, errors.New("upload failed")
			default:
				return 0, 0, errors.New("unexpected node")
			}
		},
	})
	cp := &ControlPlaneService{
		Pool:     pool,
		SubMgr:   subMgr,
		GeoIP:    &geoip.Service{},
		ProbeMgr: probeMgr,
	}

	result, err := cp.ProbeQualityBatch(NodeFilters{SubscriptionID: &sub.ID}, 1000, 10, 5, false, false)
	if err != nil {
		t.Fatalf("ProbeQualityBatch: %v", err)
	}
	if result.MatchedCount != 4 || result.TestedCount != 3 || result.DisabledCount != 2 ||
		result.KeptCount != 2 || result.LatencyFailedCount != 1 || result.BandwidthFailedCount != 1 ||
		result.LatencyThresholdFailedCount != 1 || result.BandwidthThresholdFailedCount != 1 {
		t.Fatalf("quality result = %+v", result)
	}
	if len(result.FailureSamples) != 3 {
		t.Fatalf("failure samples len = %d, want 3; result=%+v", len(result.FailureSamples), result)
	}
	if pool.IsNodeDisabled(goodHash) {
		t.Fatal("good node should remain enabled")
	}
	if !pool.IsNodeDisabled(highLatencyHash) || !pool.IsNodeDisabled(lowBandwidthHash) {
		t.Fatal("nodes failing either quality threshold should be disabled")
	}
	if pool.IsNodeDisabled(failedHash) {
		t.Fatal("node with unknown quality should not be disabled")
	}

	result, err = cp.ProbeQualityBatch(NodeFilters{SubscriptionID: &sub.ID}, 1000, 10, 5, true, false)
	if err != nil {
		t.Fatalf("ProbeQualityBatch(strict): %v", err)
	}
	if result.FailedDisabledCount != 1 {
		t.Fatalf("FailedDisabledCount = %d, want 1; result=%+v", result.FailedDisabledCount, result)
	}
	if result.KeptCount != 1 || len(result.FailureSamples) != 1 {
		t.Fatalf("strict diagnostics mismatch: result=%+v", result)
	}
	if got := result.FailureSamples[0].Reasons; len(got) != 2 || got[0] != "latency_probe_failed" || got[1] != "bandwidth_probe_failed" {
		t.Fatalf("strict failure sample reasons = %#v", got)
	}
	if !result.FailureSamples[0].Disabled {
		t.Fatalf("strict failure sample should be disabled: %+v", result.FailureSamples[0])
	}
	if !pool.IsNodeDisabled(failedHash) {
		t.Fatal("strict mode should disable nodes whose quality cannot be verified")
	}
}

func TestProbeQualityBatch_RecoverDisabledReenablesPassingManualDisabledNode(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("sub-recover-quality", "sub-recover-quality", "https://example.com/recover", true, false)
	subMgr.Register(sub)

	recoverHash := addRoutableNodeForSubscription(
		t, pool, sub, []byte(`{"id":"quality-recover"}`), "203.0.113.50",
	)
	entry, ok := pool.GetEntry(recoverHash)
	if !ok {
		t.Fatal("recover node missing")
	}
	entry.FailureCount.Store(1)
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Minute).UnixNano())
	if !pool.SetNodeManualDisabled(recoverHash, true) {
		t.Fatal("expected manual disable to change node")
	}

	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool: pool,
		Fetcher: func(hash node.Hash, _ string) ([]byte, time.Duration, error) {
			if hash != recoverHash {
				return nil, 0, errors.New("unexpected node")
			}
			return []byte("ok"), 90 * time.Millisecond, nil
		},
		BandwidthFetcher: func(hash node.Hash, _ string, _ int64) (int64, time.Duration, error) {
			if hash != recoverHash {
				return 0, 0, errors.New("unexpected node")
			}
			return 5_000_000, 2 * time.Second, nil
		},
		UploadFetcher: func(hash node.Hash, _ string, _ int64) (int64, time.Duration, error) {
			if hash != recoverHash {
				return 0, 0, errors.New("unexpected node")
			}
			return 2_000_000, time.Second, nil
		},
	})
	cp := &ControlPlaneService{
		Pool:     pool,
		SubMgr:   subMgr,
		GeoIP:    &geoip.Service{},
		ProbeMgr: probeMgr,
	}

	result, err := cp.ProbeQualityBatch(NodeFilters{SubscriptionID: &sub.ID}, 1000, 10, 5, true, true)
	if err != nil {
		t.Fatalf("ProbeQualityBatch(recover): %v", err)
	}
	if result.MatchedCount != 1 || result.TestedCount != 1 || result.KeptCount != 1 || result.ReenabledCount != 1 || result.DisabledCount != 0 {
		t.Fatalf("recover result = %+v", result)
	}
	if pool.IsNodeDisabled(recoverHash) {
		t.Fatal("passing manual-disabled node should be re-enabled")
	}
	if got := entry.FailureCount.Load(); got != 0 {
		t.Fatalf("failure count = %d, want 0", got)
	}
	if got := entry.CircuitOpenSince.Load(); got != 0 {
		t.Fatalf("circuit open since = %d, want 0", got)
	}
}

func TestUpdateNode_RejectsInvalidPatch(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)
	hash := addRoutableNodeForSubscription(t, pool, sub, []byte(`{"type":"ss","server":"1.1.1.1","port":443}`), "203.0.113.30")

	cp := &ControlPlaneService{Pool: pool, SubMgr: subMgr, GeoIP: &geoip.Service{}}

	for _, patch := range [][]byte{
		[]byte(`{}`),
		[]byte(`{"manual_disabled":null}`),
		[]byte(`{"manual_disabled":"true"}`),
		[]byte(`{"enabled":false}`),
	} {
		if _, err := cp.UpdateNode(hash.Hex(), patch); err == nil {
			t.Fatalf("UpdateNode(%s) expected error", patch)
		}
	}
}

func TestListNodes_ProbedSinceUsesLastLatencyProbeAttempt(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing", hash.Hex())
	}

	latencyAttempt := time.Now().Add(-2 * time.Minute).UnixNano()
	entry.LastLatencyProbeAttempt.Store(latencyAttempt)
	// Keep egress update older to ensure filter is using LastLatencyProbeAttempt.
	entry.LastEgressUpdate.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	before := time.Unix(0, latencyAttempt).Add(-1 * time.Minute)
	nodes, err := cp.ListNodes(NodeFilters{ProbedSince: &before})
	if err != nil {
		t.Fatalf("ListNodes(before): %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("ListNodes(before) len = %d, want 1", len(nodes))
	}

	after := time.Unix(0, latencyAttempt).Add(1 * time.Minute)
	nodes, err = cp.ListNodes(NodeFilters{ProbedSince: &after})
	if err != nil {
		t.Fatalf("ListNodes(after): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("ListNodes(after) len = %d, want 0", len(nodes))
	}
}

func TestListNodes_TagKeywordFuzzyMatchIsCaseInsensitive(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	matchHash := addRoutableNodeForSubscriptionWithTag(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
		"hongkong-fast-01",
	)
	_ = addRoutableNodeForSubscriptionWithTag(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.31",
		"japan-slow-01",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	keyword := "FAST"
	nodes, err := cp.ListNodes(NodeFilters{TagKeyword: &keyword})
	if err != nil {
		t.Fatalf("ListNodes(tag_keyword): %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("ListNodes(tag_keyword) len = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != matchHash.Hex() {
		t.Fatalf("ListNodes(tag_keyword) hash = %q, want %q", nodes[0].NodeHash, matchHash.Hex())
	}
}

func TestListNodes_RegionFilterAndSummaryPreferStoredRegion(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.40",
	)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing", hash.Hex())
	}
	entry.SetEgressRegion("jp")

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{}, // empty service returns "", forcing stored-region path
	}

	region := "jp"
	nodes, err := cp.ListNodes(NodeFilters{Region: &region})
	if err != nil {
		t.Fatalf("ListNodes(region): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != hash.Hex() {
		t.Fatalf("region-filtered nodes = %+v, want [%s]", nodes, hash.Hex())
	}

	got, err := cp.GetNode(hash.Hex())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Region != "jp" {
		t.Fatalf("summary region: got %q, want %q", got.Region, "jp")
	}
}

func TestListNodes_EnabledFilter(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	subEnabled := subscription.NewSubscription("sub-enabled", "sub-enabled", "https://example.com/enabled", true, false)
	subDisabled := subscription.NewSubscription("sub-disabled", "sub-disabled", "https://example.com/disabled", false, false)
	subMgr.Register(subEnabled)
	subMgr.Register(subDisabled)

	enabledHash := addRoutableNodeForSubscription(
		t,
		pool,
		subEnabled,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.70",
	)
	disabledHash := addRoutableNodeForSubscription(
		t,
		pool,
		subDisabled,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.71",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	enabled := true
	nodes, err := cp.ListNodes(NodeFilters{Enabled: &enabled})
	if err != nil {
		t.Fatalf("ListNodes(enabled=true): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != enabledHash.Hex() {
		t.Fatalf("enabled filter result = %+v, want [%s]", nodes, enabledHash.Hex())
	}

	disabled := false
	nodes, err = cp.ListNodes(NodeFilters{Enabled: &disabled})
	if err != nil {
		t.Fatalf("ListNodes(enabled=false): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != disabledHash.Hex() {
		t.Fatalf("disabled filter result = %+v, want [%s]", nodes, disabledHash.Hex())
	}
}

func TestProbeEgress_ReturnsRegion(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.60",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{}, // empty service keeps focus on stored region from loc
		ProbeMgr: probe.NewProbeManager(probe.ProbeConfig{
			Pool: pool,
			Fetcher: func(_ node.Hash, _ string) ([]byte, time.Duration, error) {
				return []byte("ip=198.51.100.88\nloc=JP"), 20 * time.Millisecond, nil
			},
		}),
	}

	got, err := cp.ProbeEgress(hash.Hex())
	if err != nil {
		t.Fatalf("ProbeEgress: %v", err)
	}
	if got.EgressIP != "198.51.100.88" {
		t.Fatalf("egress_ip: got %q, want %q", got.EgressIP, "198.51.100.88")
	}
	if got.Region != "jp" {
		t.Fatalf("region: got %q, want %q", got.Region, "jp")
	}
}

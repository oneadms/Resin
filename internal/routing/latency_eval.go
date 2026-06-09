package routing

import (
	"time"

	"github.com/Resinat/Resin/internal/node"
)

const (
	bandwidthFreshnessWindow    = 24 * time.Hour
	representativeResponseBytes = 4_000_000
	representativeRequestBytes  = 500_000
	unknownLatencyPenalty       = 1500 * time.Millisecond
	unknownBandwidthMbps        = 1.0
	unknownUploadBandwidthMbps  = 1.0
)

func lookupRecentDomainLatency(
	entry *node.NodeEntry,
	domain string,
	now time.Time,
	window time.Duration,
) (time.Duration, bool) {
	if entry == nil || entry.LatencyTable == nil || domain == "" {
		return 0, false
	}
	stats, ok := entry.LatencyTable.GetDomainStats(domain)
	if !ok || !isRecent(stats.LastUpdated, now, window) {
		return 0, false
	}
	return stats.Ewma, true
}

func averageRecentAuthorityLatency(
	entry *node.NodeEntry,
	authorities []string,
	now time.Time,
	window time.Duration,
) (time.Duration, bool) {
	if entry == nil || entry.LatencyTable == nil || len(authorities) == 0 {
		return 0, false
	}
	var sum time.Duration
	var count int64
	for _, domain := range authorities {
		latency, ok := lookupRecentDomainLatency(entry, domain, now, window)
		if !ok {
			continue
		}
		sum += latency
		count++
	}
	if count == 0 {
		return 0, false
	}
	return time.Duration(int64(sum) / count), true
}

func averageComparableAuthorityLatencies(
	e1, e2 *node.NodeEntry,
	authorities []string,
	now time.Time,
	window time.Duration,
) (time.Duration, time.Duration, bool) {
	if e1 == nil || e2 == nil || e1.LatencyTable == nil || e2.LatencyTable == nil || len(authorities) == 0 {
		return 0, 0, false
	}
	var sum1, sum2 time.Duration
	var count int64
	for _, domain := range authorities {
		latency1, ok1 := lookupRecentDomainLatency(e1, domain, now, window)
		latency2, ok2 := lookupRecentDomainLatency(e2, domain, now, window)
		if !ok1 || !ok2 {
			continue
		}
		sum1 += latency1
		sum2 += latency2
		count++
	}
	if count == 0 {
		return 0, 0, false
	}
	return time.Duration(int64(sum1) / count), time.Duration(int64(sum2) / count), true
}

func recentBandwidthMbps(entry *node.NodeEntry, now time.Time) (float64, bool) {
	if entry == nil {
		return 0, false
	}
	updatedNs := entry.LastBandwidthUpdate.Load()
	mbps := entry.BandwidthMbps()
	if updatedNs <= 0 || mbps <= 0 || now.Sub(time.Unix(0, updatedNs)) > bandwidthFreshnessWindow {
		return 0, false
	}
	return mbps, true
}

func recentUploadBandwidthMbps(entry *node.NodeEntry, now time.Time) (float64, bool) {
	if entry == nil {
		return 0, false
	}
	updatedNs := entry.LastBandwidthUpdate.Load()
	mbps := entry.UploadBandwidthMbps()
	if updatedNs <= 0 || mbps <= 0 || now.Sub(time.Unix(0, updatedNs)) > bandwidthFreshnessWindow {
		return 0, false
	}
	return mbps, true
}

// performanceCostMs estimates time-to-first-byte plus transfer time for a
// representative AI response. Lower is better.
func performanceCostMs(latency time.Duration, downloadMbps float64, uploadMbps float64) float64 {
	cost := float64(latency) / float64(time.Millisecond)
	if uploadMbps > 0 {
		cost += float64(representativeRequestBytes*8) / uploadMbps / 1000
	}
	if downloadMbps > 0 {
		cost += float64(representativeResponseBytes*8) / downloadMbps / 1000
	}
	return cost
}

func entryPerformanceCostMs(
	entry *node.NodeEntry,
	target string,
	authorities []string,
	now time.Time,
	window time.Duration,
) float64 {
	latency := unknownLatencyPenalty
	if domainLatency, ok := lookupRecentDomainLatency(entry, target, now, window); ok {
		latency = domainLatency
	} else if authorityLatency, ok := averageRecentAuthorityLatency(entry, authorities, now, window); ok {
		latency = authorityLatency
	}

	bandwidth := unknownBandwidthMbps
	if measuredBandwidth, ok := recentBandwidthMbps(entry, now); ok {
		bandwidth = measuredBandwidth
	}
	uploadBandwidth := unknownUploadBandwidthMbps
	if measuredUploadBandwidth, ok := recentUploadBandwidthMbps(entry, now); ok {
		uploadBandwidth = measuredUploadBandwidth
	}
	return performanceCostMs(latency, bandwidth, uploadBandwidth)
}

func comparePerformanceCosts(
	h1, h2 node.Hash,
	pool PoolAccessor,
	target string,
	authorities []string,
	window time.Duration,
) (float64, float64) {
	e1, ok1 := pool.GetEntry(h1)
	e2, ok2 := pool.GetEntry(h2)
	return compareEntryPerformanceCosts(e1, ok1, e2, ok2, target, authorities, window)
}

func compareEntryPerformanceCosts(
	e1 *node.NodeEntry,
	ok1 bool,
	e2 *node.NodeEntry,
	ok2 bool,
	target string,
	authorities []string,
	window time.Duration,
) (float64, float64) {
	if !ok1 || !ok2 {
		return 0, 0
	}

	now := time.Now()
	return entryPerformanceCostMs(e1, target, authorities, now, window),
		entryPerformanceCostMs(e2, target, authorities, now, window)
}

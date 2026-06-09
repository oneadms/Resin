package service

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/subscription"
)

// ------------------------------------------------------------------
// Nodes
// ------------------------------------------------------------------

var nodePatchAllowedFields = map[string]bool{
	"manual_disabled": true,
}

// NodeFilters holds query filters for listing nodes.
type NodeFilters struct {
	PlatformID     *string
	SubscriptionID *string
	Enabled        *bool
	Region         *string
	CircuitOpen    *bool
	HasOutbound    *bool
	EgressIP       *string
	ProbedSince    *time.Time
	TagKeyword     *string
}

// ListNodes returns nodes from the pool with optional filters.
func (s *ControlPlaneService) ListNodes(filters NodeFilters) ([]NodeSummary, error) {
	var subLookup node.SubLookupFunc
	if s != nil && s.Pool != nil {
		subLookup = s.Pool.MakeSubLookup()
	}

	// If platform_id filter, get the platform view.
	var platformView map[node.Hash]struct{}
	if filters.PlatformID != nil {
		plat, ok := s.Pool.GetPlatform(*filters.PlatformID)
		if !ok {
			return nil, notFound("platform not found")
		}
		platformView = make(map[node.Hash]struct{}, plat.View().Size())
		plat.View().Range(func(h node.Hash) bool {
			platformView[h] = struct{}{}
			return true
		})
	}

	var subNodes map[node.Hash]struct{}
	if filters.SubscriptionID != nil {
		sub := s.SubMgr.Lookup(*filters.SubscriptionID)
		if sub == nil {
			return nil, notFound("subscription not found")
		}
		subNodes = make(map[node.Hash]struct{})
		sub.ManagedNodes().RangeNodes(func(h node.Hash, managed subscription.ManagedNode) bool {
			if managed.Evicted {
				return true
			}
			subNodes[h] = struct{}{}
			return true
		})
	}

	var result []NodeSummary
	appendIfMatched := func(h node.Hash, entry *node.NodeEntry) {
		if !s.nodeEntryMatchesFilters(entry, filters, subLookup) {
			return
		}
		result = append(result, s.nodeEntryToSummary(h, entry))
	}

	appendIfMatchedHash := func(h node.Hash) {
		entry, ok := s.Pool.GetEntry(h)
		if !ok {
			return
		}
		appendIfMatched(h, entry)
	}

	switch {
	case platformView != nil && subNodes != nil:
		// Iterate the smaller candidate set, then intersect by membership.
		if len(platformView) <= len(subNodes) {
			for h := range platformView {
				if _, ok := subNodes[h]; !ok {
					continue
				}
				appendIfMatchedHash(h)
			}
		} else {
			for h := range subNodes {
				if _, ok := platformView[h]; !ok {
					continue
				}
				appendIfMatchedHash(h)
			}
		}
	case platformView != nil:
		for h := range platformView {
			appendIfMatchedHash(h)
		}
	case subNodes != nil:
		for h := range subNodes {
			appendIfMatchedHash(h)
		}
	default:
		s.Pool.Range(func(h node.Hash, entry *node.NodeEntry) bool {
			appendIfMatched(h, entry)
			return true
		})
	}

	if result == nil {
		result = []NodeSummary{}
	}
	return result, nil
}

func (s *ControlPlaneService) nodeEntryMatchesFilters(
	entry *node.NodeEntry,
	filters NodeFilters,
	subLookup node.SubLookupFunc,
) bool {
	// Enabled/disabled filter.
	if filters.Enabled != nil {
		enabled := !entry.IsManuallyDisabled()
		if subLookup != nil {
			enabled = !entry.IsDisabled(subLookup)
		}
		if enabled != *filters.Enabled {
			return false
		}
	}

	// Node tag fuzzy search filter.
	if filters.TagKeyword != nil {
		keyword := strings.ToLower(strings.TrimSpace(*filters.TagKeyword))
		if keyword != "" {
			matched := false
			for _, subID := range entry.SubscriptionIDs() {
				sub := s.SubMgr.Lookup(subID)
				if sub == nil {
					continue
				}
				managed, ok := sub.ManagedNodes().LoadNode(entry.Hash)
				if !ok {
					continue
				}
				tags := managed.Tags
				for _, tag := range tags {
					displayTag := sub.Name() + "/" + tag
					if strings.Contains(strings.ToLower(displayTag), keyword) {
						matched = true
						break
					}
				}
				if matched {
					break
				}
			}
			if !matched {
				return false
			}
		}
	}

	// Region filter.
	if filters.Region != nil {
		region := entry.GetRegion(nil)
		if s.GeoIP != nil {
			region = entry.GetRegion(s.GeoIP.Lookup)
		}
		if region == "" || region != *filters.Region {
			return false
		}
	}
	// Circuit open filter.
	if filters.CircuitOpen != nil {
		if entry.IsCircuitOpen() != *filters.CircuitOpen {
			return false
		}
	}
	// Has outbound filter.
	if filters.HasOutbound != nil {
		if entry.HasOutbound() != *filters.HasOutbound {
			return false
		}
	}
	// Egress IP filter.
	if filters.EgressIP != nil {
		egressIP := entry.GetEgressIP()
		if !egressIP.IsValid() || egressIP.String() != *filters.EgressIP {
			return false
		}
	}
	// Probed since filter.
	if filters.ProbedSince != nil {
		lastUpdate := entry.LastLatencyProbeAttempt.Load()
		if lastUpdate < filters.ProbedSince.UnixNano() {
			return false
		}
	}
	return true
}

// GetNode returns a single node by hash.
func (s *ControlPlaneService) GetNode(hashStr string) (*NodeSummary, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	entry, ok := s.Pool.GetEntry(h)
	if !ok {
		return nil, notFound("node not found")
	}
	ns := s.nodeEntryToSummary(h, entry)
	return &ns, nil
}

// UpdateNode applies a constrained partial patch to a node.
func (s *ControlPlaneService) UpdateNode(hashStr string, patchJSON json.RawMessage) (*NodeSummary, error) {
	patch, verr := parseMergePatch(patchJSON)
	if verr != nil {
		return nil, verr
	}
	if err := patch.validateFields(nodePatchAllowedFields, func(key string) string {
		return fmt.Sprintf("field %q is read-only or unknown", key)
	}); err != nil {
		return nil, err
	}

	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	entry, ok := s.Pool.GetEntry(h)
	if !ok {
		return nil, notFound("node not found")
	}

	if disabled, ok, err := patch.optionalBool("manual_disabled"); err != nil {
		return nil, err
	} else if ok {
		s.Pool.SetNodeManualDisabled(h, disabled)
	}

	ns := s.nodeEntryToSummary(h, entry)
	return &ns, nil
}

// ProbeEgress triggers a synchronous egress probe and returns results.
func (s *ControlPlaneService) ProbeEgress(hashStr string) (*probe.EgressProbeResult, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	entry, ok := s.Pool.GetEntry(h)
	if !ok {
		return nil, notFound("node not found")
	}
	result, err := s.ProbeMgr.ProbeEgressSync(h)
	if err != nil {
		return nil, internal("egress probe failed", err)
	}
	result.Region = entry.GetRegion(nil)
	if s.GeoIP != nil {
		result.Region = entry.GetRegion(s.GeoIP.Lookup)
	}
	return result, nil
}

// ProbeLatency triggers a synchronous latency probe and returns results.
func (s *ControlPlaneService) ProbeLatency(hashStr string) (*probe.LatencyProbeResult, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	if _, ok := s.Pool.GetEntry(h); !ok {
		return nil, notFound("node not found")
	}
	result, err := s.ProbeMgr.ProbeLatencySync(h)
	if err != nil {
		return nil, internal("latency probe failed", err)
	}
	return result, nil
}

// ProbeBandwidth triggers a synchronous download bandwidth probe.
func (s *ControlPlaneService) ProbeBandwidth(hashStr string) (*probe.BandwidthProbeResult, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	if _, ok := s.Pool.GetEntry(h); !ok {
		return nil, notFound("node not found")
	}
	result, err := s.ProbeMgr.ProbeBandwidthSync(h)
	if err != nil {
		return nil, internal("bandwidth probe failed", err)
	}
	return result, nil
}

// BatchLatencyProbeResult summarizes a bulk real-connection latency probe.
type BatchLatencyProbeResult struct {
	MatchedCount  int `json:"matched_count"`
	TestedCount   int `json:"tested_count"`
	DisabledCount int `json:"disabled_count"`
	FailedCount   int `json:"failed_count"`
	SkippedCount  int `json:"skipped_count"`
}

// ProbeLatencyBatch probes all enabled nodes matching filters and manually
// disables nodes whose measured latency exceeds maxLatencyMs.
func (s *ControlPlaneService) ProbeLatencyBatch(filters NodeFilters, maxLatencyMs float64) (*BatchLatencyProbeResult, error) {
	if maxLatencyMs <= 0 || math.IsNaN(maxLatencyMs) || math.IsInf(maxLatencyMs, 0) {
		return nil, invalidArg("max_latency_ms: must be a positive finite number")
	}
	if s == nil || s.ProbeMgr == nil {
		return nil, internal("latency probe manager is unavailable", nil)
	}

	nodes, err := s.ListNodes(filters)
	if err != nil {
		return nil, err
	}

	result := &BatchLatencyProbeResult{MatchedCount: len(nodes)}
	const concurrency = 8
	tasks := make(chan NodeSummary)
	var tested atomic.Int64
	var disabled atomic.Int64
	var failed atomic.Int64
	var skipped atomic.Int64
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for summary := range tasks {
			if !summary.Enabled {
				skipped.Add(1)
				continue
			}
			probeResult, probeErr := s.ProbeLatency(summary.NodeHash)
			if probeErr != nil {
				failed.Add(1)
				continue
			}
			tested.Add(1)
			if probeResult.LatencyMs > maxLatencyMs {
				h, parseErr := node.ParseHex(summary.NodeHash)
				if parseErr == nil && s.Pool.SetNodeManualDisabled(h, true) {
					disabled.Add(1)
				}
			}
		}
	}

	workerCount := min(concurrency, len(nodes))
	for range workerCount {
		wg.Add(1)
		go worker()
	}
	for _, summary := range nodes {
		tasks <- summary
	}
	close(tasks)
	wg.Wait()

	result.TestedCount = int(tested.Load())
	result.DisabledCount = int(disabled.Load())
	result.FailedCount = int(failed.Load())
	result.SkippedCount = int(skipped.Load())
	return result, nil
}

// BatchBandwidthProbeResult summarizes a bulk download bandwidth probe.
type BatchBandwidthProbeResult struct {
	MatchedCount  int `json:"matched_count"`
	TestedCount   int `json:"tested_count"`
	DisabledCount int `json:"disabled_count"`
	FailedCount   int `json:"failed_count"`
	SkippedCount  int `json:"skipped_count"`
}

// BatchQualityProbeResult summarizes combined latency and bandwidth screening.
type BatchQualityProbeResult struct {
	MatchedCount                        int                       `json:"matched_count"`
	TestedCount                         int                       `json:"tested_count"`
	KeptCount                           int                       `json:"kept_count"`
	ReenabledCount                      int                       `json:"reenabled_count"`
	DisabledCount                       int                       `json:"disabled_count"`
	FailedDisabledCount                 int                       `json:"failed_disabled_count"`
	LatencyFailedCount                  int                       `json:"latency_failed_count"`
	LatencyThresholdFailedCount         int                       `json:"latency_threshold_failed_count"`
	BandwidthFailedCount                int                       `json:"bandwidth_failed_count"`
	BandwidthThresholdFailedCount       int                       `json:"bandwidth_threshold_failed_count"`
	UploadBandwidthThresholdFailedCount int                       `json:"upload_bandwidth_threshold_failed_count"`
	SkippedCount                        int                       `json:"skipped_count"`
	FailureSamples                      []QualityProbeNodeFailure `json:"failure_samples,omitempty"`
}

// QualityProbeNodeFailure captures a bounded diagnostic sample for nodes that
// could not prove they are good enough for large AI responses.
type QualityProbeNodeFailure struct {
	NodeHash       string   `json:"node_hash"`
	DisplayTag     string   `json:"display_tag,omitempty"`
	Reasons        []string `json:"reasons"`
	Disabled       bool     `json:"disabled"`
	LatencyMs      *float64 `json:"latency_ms,omitempty"`
	DownloadMbps   *float64 `json:"download_mbps,omitempty"`
	UploadMbps     *float64 `json:"upload_mbps,omitempty"`
	LatencyError   string   `json:"latency_error,omitempty"`
	BandwidthError string   `json:"bandwidth_error,omitempty"`
}

// ProbeBandwidthBatch probes all enabled nodes matching filters and manually
// disables nodes whose measured download or upload speed is below threshold.
func (s *ControlPlaneService) ProbeBandwidthBatch(filters NodeFilters, minDownloadMbps float64, minUploadMbps float64) (*BatchBandwidthProbeResult, error) {
	if minDownloadMbps <= 0 || math.IsNaN(minDownloadMbps) || math.IsInf(minDownloadMbps, 0) {
		return nil, invalidArg("min_download_mbps: must be a positive finite number")
	}
	if minUploadMbps <= 0 || math.IsNaN(minUploadMbps) || math.IsInf(minUploadMbps, 0) {
		return nil, invalidArg("min_upload_mbps: must be a positive finite number")
	}
	if s == nil || s.ProbeMgr == nil {
		return nil, internal("bandwidth probe manager is unavailable", nil)
	}

	nodes, err := s.ListNodes(filters)
	if err != nil {
		return nil, err
	}

	result := &BatchBandwidthProbeResult{MatchedCount: len(nodes)}
	const concurrency = 4
	tasks := make(chan NodeSummary)
	var tested atomic.Int64
	var disabled atomic.Int64
	var failed atomic.Int64
	var skipped atomic.Int64
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for summary := range tasks {
			if !summary.Enabled {
				skipped.Add(1)
				continue
			}
			probeResult, probeErr := s.ProbeBandwidth(summary.NodeHash)
			if probeErr != nil {
				failed.Add(1)
				continue
			}
			tested.Add(1)
			if probeResult.DownloadMbps < minDownloadMbps || probeResult.UploadMbps < minUploadMbps {
				h, parseErr := node.ParseHex(summary.NodeHash)
				if parseErr == nil && s.Pool.SetNodeManualDisabled(h, true) {
					disabled.Add(1)
				}
			}
		}
	}

	workerCount := min(concurrency, len(nodes))
	for range workerCount {
		wg.Add(1)
		go worker()
	}
	for _, summary := range nodes {
		tasks <- summary
	}
	close(tasks)
	wg.Wait()

	result.TestedCount = int(tested.Load())
	result.DisabledCount = int(disabled.Load())
	result.FailedCount = int(failed.Load())
	result.SkippedCount = int(skipped.Load())
	return result, nil
}

// ProbeQualityBatch measures latency plus download/upload bandwidth, then
// disables nodes that fail any successfully measured threshold.
func (s *ControlPlaneService) ProbeQualityBatch(
	filters NodeFilters,
	maxLatencyMs float64,
	minDownloadMbps float64,
	minUploadMbps float64,
	disableFailed bool,
	recoverDisabled bool,
) (*BatchQualityProbeResult, error) {
	if maxLatencyMs <= 0 || math.IsNaN(maxLatencyMs) || math.IsInf(maxLatencyMs, 0) {
		return nil, invalidArg("max_latency_ms: must be a positive finite number")
	}
	if minDownloadMbps <= 0 || math.IsNaN(minDownloadMbps) || math.IsInf(minDownloadMbps, 0) {
		return nil, invalidArg("min_download_mbps: must be a positive finite number")
	}
	if minUploadMbps <= 0 || math.IsNaN(minUploadMbps) || math.IsInf(minUploadMbps, 0) {
		return nil, invalidArg("min_upload_mbps: must be a positive finite number")
	}
	if s == nil || s.ProbeMgr == nil {
		return nil, internal("probe manager is unavailable", nil)
	}

	nodes, err := s.ListNodes(filters)
	if err != nil {
		return nil, err
	}

	result := &BatchQualityProbeResult{MatchedCount: len(nodes)}
	const concurrency = 4
	const maxFailureSamples = 10
	tasks := make(chan NodeSummary)
	var tested atomic.Int64
	var kept atomic.Int64
	var reenabled atomic.Int64
	var disabled atomic.Int64
	var failedDisabled atomic.Int64
	var latencyFailed atomic.Int64
	var latencyThresholdFailed atomic.Int64
	var bandwidthFailed atomic.Int64
	var bandwidthThresholdFailed atomic.Int64
	var uploadBandwidthThresholdFailed atomic.Int64
	var sampleMu sync.Mutex
	var skipped atomic.Int64
	var wg sync.WaitGroup

	recordFailureSample := func(summary NodeSummary, reasons []string, disabled bool, latencyMs *float64, downloadMbps *float64, uploadMbps *float64, latencyErr error, bandwidthErr error) {
		if len(reasons) == 0 {
			return
		}
		sampleMu.Lock()
		defer sampleMu.Unlock()
		if len(result.FailureSamples) >= maxFailureSamples {
			return
		}
		displayTag := summary.DisplayTag
		if displayTag == "" && len(summary.Tags) > 0 {
			displayTag = summary.Tags[0].Tag
		}
		sample := QualityProbeNodeFailure{
			NodeHash:     summary.NodeHash,
			DisplayTag:   displayTag,
			Reasons:      append([]string(nil), reasons...),
			Disabled:     disabled,
			LatencyMs:    latencyMs,
			DownloadMbps: downloadMbps,
			UploadMbps:   uploadMbps,
		}
		if latencyErr != nil {
			sample.LatencyError = latencyErr.Error()
		}
		if bandwidthErr != nil {
			sample.BandwidthError = bandwidthErr.Error()
		}
		result.FailureSamples = append(result.FailureSamples, sample)
	}

	worker := func() {
		defer wg.Done()
		for summary := range tasks {
			if !summary.Enabled && !(recoverDisabled && summary.ManualDisabled) {
				skipped.Add(1)
				continue
			}

			latencyResult, latencyErr := s.ProbeLatency(summary.NodeHash)
			if latencyErr != nil {
				latencyFailed.Add(1)
			}
			bandwidthResult, bandwidthErr := s.ProbeBandwidth(summary.NodeHash)
			if bandwidthErr != nil {
				bandwidthFailed.Add(1)
			}
			if latencyErr == nil && bandwidthErr == nil {
				tested.Add(1)
			}

			var reasons []string
			var latencyMs *float64
			var downloadMbps *float64
			var uploadMbps *float64
			shouldDisable := false
			if latencyErr != nil {
				reasons = append(reasons, "latency_probe_failed")
			} else {
				latencyValue := latencyResult.LatencyMs
				latencyMs = &latencyValue
				if latencyResult.LatencyMs > maxLatencyMs {
					latencyThresholdFailed.Add(1)
					reasons = append(reasons, "latency_above_threshold")
					shouldDisable = true
				}
			}
			if bandwidthErr != nil {
				reasons = append(reasons, "bandwidth_probe_failed")
			} else {
				bandwidthValue := bandwidthResult.DownloadMbps
				downloadMbps = &bandwidthValue
				uploadValue := bandwidthResult.UploadMbps
				uploadMbps = &uploadValue
				if bandwidthResult.DownloadMbps < minDownloadMbps {
					bandwidthThresholdFailed.Add(1)
					reasons = append(reasons, "download_bandwidth_below_threshold")
					shouldDisable = true
				}
				if bandwidthResult.UploadMbps < minUploadMbps {
					uploadBandwidthThresholdFailed.Add(1)
					reasons = append(reasons, "upload_bandwidth_below_threshold")
					shouldDisable = true
				}
			}
			disabledByFailure := disableFailed && (latencyErr != nil || bandwidthErr != nil)
			shouldDisable = shouldDisable || disabledByFailure
			disabledNow := false
			reenabledNow := false
			if shouldDisable {
				h, parseErr := node.ParseHex(summary.NodeHash)
				if parseErr == nil && s.Pool.SetNodeManualDisabled(h, true) {
					disabled.Add(1)
					disabledNow = true
					if disabledByFailure {
						failedDisabled.Add(1)
					}
				}
			} else {
				if recoverDisabled && summary.ManualDisabled {
					h, parseErr := node.ParseHex(summary.NodeHash)
					if parseErr == nil && s.Pool.SetNodeManualDisabled(h, false) {
						reenabled.Add(1)
						reenabledNow = true
					}
				}
				kept.Add(1)
			}
			recordFailureSample(summary, reasons, disabledNow || (summary.ManualDisabled && !reenabledNow), latencyMs, downloadMbps, uploadMbps, latencyErr, bandwidthErr)
		}
	}

	workerCount := min(concurrency, len(nodes))
	for range workerCount {
		wg.Add(1)
		go worker()
	}
	for _, summary := range nodes {
		tasks <- summary
	}
	close(tasks)
	wg.Wait()

	result.TestedCount = int(tested.Load())
	result.KeptCount = int(kept.Load())
	result.ReenabledCount = int(reenabled.Load())
	result.DisabledCount = int(disabled.Load())
	result.FailedDisabledCount = int(failedDisabled.Load())
	result.LatencyFailedCount = int(latencyFailed.Load())
	result.LatencyThresholdFailedCount = int(latencyThresholdFailed.Load())
	result.BandwidthFailedCount = int(bandwidthFailed.Load())
	result.BandwidthThresholdFailedCount = int(bandwidthThresholdFailed.Load())
	result.UploadBandwidthThresholdFailedCount = int(uploadBandwidthThresholdFailed.Load())
	result.SkippedCount = int(skipped.Load())
	return result, nil
}

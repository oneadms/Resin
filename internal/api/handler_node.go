package api

import (
	"cmp"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/service"
)

func nodeTagSortKey(n service.NodeSummary) string {
	if n.DisplayTag != "" {
		return n.DisplayTag
	}
	if len(n.Tags) == 0 {
		return ""
	}
	bestCreated := int64(math.MaxInt64)
	bestTag := ""
	for _, t := range n.Tags {
		if t.SubscriptionCreatedAtNs < bestCreated {
			bestCreated = t.SubscriptionCreatedAtNs
			bestTag = t.Tag
			continue
		}
		if t.SubscriptionCreatedAtNs == bestCreated && (bestTag == "" || t.Tag < bestTag) {
			bestTag = t.Tag
		}
	}
	return bestTag
}

func compareNodeSummaries(sortBy string, a, b service.NodeSummary) int {
	order := 0
	switch sortBy {
	case "created_at":
		order = strings.Compare(a.CreatedAt, b.CreatedAt)
	case "failure_count":
		order = cmp.Compare(a.FailureCount, b.FailureCount)
	case "region":
		order = strings.Compare(a.Region, b.Region)
	default:
		order = strings.Compare(nodeTagSortKey(a), nodeTagSortKey(b))
	}
	if order != 0 {
		return order
	}
	return strings.Compare(a.NodeHash, b.NodeHash)
}

func sortNodeSummaries(nodes []service.NodeSummary, sorting Sorting) {
	slices.SortStableFunc(nodes, func(a, b service.NodeSummary) int {
		return applySortOrder(compareNodeSummaries(sorting.SortBy, a, b), sorting.SortOrder)
	})
}

type nodeListPageResponse struct {
	Items                  []service.NodeSummary `json:"items"`
	Total                  int                   `json:"total"`
	Limit                  int                   `json:"limit"`
	Offset                 int                   `json:"offset"`
	UniqueEgressIPs        int                   `json:"unique_egress_ips"`
	UniqueHealthyEgressIPs int                   `json:"unique_healthy_egress_ips"`
}

func countUniqueEgressIPs(nodes []service.NodeSummary) int {
	seen := make(map[string]struct{})
	for _, n := range nodes {
		if n.EgressIP == "" {
			continue
		}
		seen[n.EgressIP] = struct{}{}
	}
	return len(seen)
}

func countUniqueHealthyAndEnabledEgressIPs(nodes []service.NodeSummary) int {
	seen := make(map[string]struct{})
	for _, n := range nodes {
		if n.EgressIP == "" {
			continue
		}
		if !n.IsHealthyAndEnabled() {
			continue
		}
		seen[n.EgressIP] = struct{}{}
	}
	return len(seen)
}

func parseNodeFiltersOrWriteInvalid(w http.ResponseWriter, r *http.Request) (service.NodeFilters, bool) {
	q := r.URL.Query()
	filters := service.NodeFilters{}

	platformID, ok := parseOptionalUUIDQuery(w, r, "platform_id", "platform_id")
	if !ok {
		return service.NodeFilters{}, false
	}
	filters.PlatformID = platformID

	subscriptionID, ok := parseOptionalUUIDQuery(w, r, "subscription_id", "subscription_id")
	if !ok {
		return service.NodeFilters{}, false
	}
	filters.SubscriptionID = subscriptionID

	if v := q.Get("region"); v != "" {
		filters.Region = &v
	}
	if v := q.Get("egress_ip"); v != "" {
		filters.EgressIP = &v
	}
	if v := strings.TrimSpace(q.Get("tag_keyword")); v != "" {
		filters.TagKeyword = &v
	}

	circuitOpen, ok := parseBoolQueryOrWriteInvalid(w, r, "circuit_open")
	if !ok {
		return service.NodeFilters{}, false
	}
	filters.CircuitOpen = circuitOpen

	hasOutbound, ok := parseBoolQueryOrWriteInvalid(w, r, "has_outbound")
	if !ok {
		return service.NodeFilters{}, false
	}
	filters.HasOutbound = hasOutbound

	enabled, ok := parseBoolQueryOrWriteInvalid(w, r, "enabled")
	if !ok {
		return service.NodeFilters{}, false
	}
	filters.Enabled = enabled

	if v := q.Get("probed_since"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			writeInvalidArgument(w, "probed_since: invalid RFC3339 timestamp")
			return service.NodeFilters{}, false
		}
		filters.ProbedSince = &t
	}
	return filters, true
}

// HandleListNodes returns a handler for GET /api/v1/nodes.
func HandleListNodes(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filters, ok := parseNodeFiltersOrWriteInvalid(w, r)
		if !ok {
			return
		}

		nodes, err := cp.ListNodes(filters)
		if err != nil {
			writeServiceError(w, err)
			return
		}

		sorting, ok := parseSortingOrWriteInvalid(w, r, []string{"tag", "created_at", "failure_count", "region"}, "tag", "asc")
		if !ok {
			return
		}
		sortNodeSummaries(nodes, sorting)

		pg, ok := parsePaginationOrWriteInvalid(w, r)
		if !ok {
			return
		}
		WriteJSON(w, http.StatusOK, nodeListPageResponse{
			Items:                  PaginateSlice(nodes, pg),
			Total:                  len(nodes),
			Limit:                  pg.Limit,
			Offset:                 pg.Offset,
			UniqueEgressIPs:        countUniqueEgressIPs(nodes),
			UniqueHealthyEgressIPs: countUniqueHealthyAndEnabledEgressIPs(nodes),
		})
	}
}

type batchProbeLatencyRequest struct {
	MaxLatencyMs float64 `json:"max_latency_ms"`
}

type batchProbeBandwidthRequest struct {
	MinDownloadMbps float64 `json:"min_download_mbps"`
}

type batchProbeQualityRequest struct {
	MaxLatencyMs    float64 `json:"max_latency_ms"`
	MinDownloadMbps float64 `json:"min_download_mbps"`
	DisableFailed   bool    `json:"disable_failed"`
	RecoverDisabled bool    `json:"recover_disabled"`
}

// HandleBatchProbeLatency returns a handler for POST /api/v1/nodes/actions/probe-latency.
func HandleBatchProbeLatency(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filters, ok := parseNodeFiltersOrWriteInvalid(w, r)
		if !ok {
			return
		}
		var req batchProbeLatencyRequest
		if err := DecodeBody(r, &req); err != nil {
			writeDecodeBodyError(w, err)
			return
		}
		result, err := cp.ProbeLatencyBatch(filters, req.MaxLatencyMs)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, result)
	}
}

// HandleBatchProbeBandwidth returns a handler for POST /api/v1/nodes/actions/probe-bandwidth.
func HandleBatchProbeBandwidth(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filters, ok := parseNodeFiltersOrWriteInvalid(w, r)
		if !ok {
			return
		}
		var req batchProbeBandwidthRequest
		if err := DecodeBody(r, &req); err != nil {
			writeDecodeBodyError(w, err)
			return
		}
		result, err := cp.ProbeBandwidthBatch(filters, req.MinDownloadMbps)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, result)
	}
}

// HandleBatchProbeQuality returns a handler for POST /api/v1/nodes/actions/probe-quality.
func HandleBatchProbeQuality(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filters, ok := parseNodeFiltersOrWriteInvalid(w, r)
		if !ok {
			return
		}
		var req batchProbeQualityRequest
		if err := DecodeBody(r, &req); err != nil {
			writeDecodeBodyError(w, err)
			return
		}
		result, err := cp.ProbeQualityBatch(filters, req.MaxLatencyMs, req.MinDownloadMbps, req.DisableFailed, req.RecoverDisabled)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, result)
	}
}

// HandleGetNode returns a handler for GET /api/v1/nodes/{hash}.
func HandleGetNode(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash := PathParam(r, "hash")
		n, err := cp.GetNode(hash)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, n)
	}
}

// HandleUpdateNode returns a handler for PATCH /api/v1/nodes/{hash}.
func HandleUpdateNode(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash := PathParam(r, "hash")
		body, ok := readRawBodyOrWriteInvalid(w, r)
		if !ok {
			return
		}
		n, err := cp.UpdateNode(hash, body)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, n)
	}
}

// HandleProbeEgress returns a handler for POST /api/v1/nodes/{hash}/actions/probe-egress.
func HandleProbeEgress(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash := PathParam(r, "hash")
		result, err := cp.ProbeEgress(hash)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, result)
	}
}

// HandleProbeLatency returns a handler for POST /api/v1/nodes/{hash}/actions/probe-latency.
func HandleProbeLatency(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash := PathParam(r, "hash")
		result, err := cp.ProbeLatency(hash)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, result)
	}
}

// HandleProbeBandwidth returns a handler for POST /api/v1/nodes/{hash}/actions/probe-bandwidth.
func HandleProbeBandwidth(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash := PathParam(r, "hash")
		result, err := cp.ProbeBandwidth(hash)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, result)
	}
}

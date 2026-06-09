import { apiRequest } from "../../lib/api-client";
import type {
  BandwidthProbeResult,
  BatchBandwidthProbeResult,
  BatchLatencyProbeResult,
  BatchQualityProbeResult,
  EgressProbeResult,
  LatencyProbeResult,
  NodeListFilters,
  NodeListQuery,
  NodeSummary,
  PageResponse,
} from "./types";

const basePath = "/api/v1/nodes";

type ApiNodeSummary = Omit<NodeSummary, "tags"> & {
  tags?: NodeSummary["tags"] | null;
  enabled?: boolean | null;
  manual_disabled?: boolean | null;
  display_tag?: string | null;
  last_error?: string | null;
  circuit_open_since?: string | null;
  egress_ip?: string | null;
  reference_latency_ms?: number | null;
  download_bandwidth_mbps?: number | null;
  region?: string | null;
  last_egress_update?: string | null;
  last_latency_probe_attempt?: string | null;
  last_authority_latency_probe_attempt?: string | null;
  last_bandwidth_probe_attempt?: string | null;
  last_bandwidth_update?: string | null;
  last_egress_update_attempt?: string | null;
};

function normalizeNode(raw: ApiNodeSummary): NodeSummary {
  const { reference_latency_ms, download_bandwidth_mbps, ...rest } = raw;
  const normalized: NodeSummary = {
    ...rest,
    enabled: raw.enabled !== false,
    manual_disabled: raw.manual_disabled === true,
    display_tag: raw.display_tag || "",
    tags: Array.isArray(raw.tags) ? raw.tags : [],
    last_error: raw.last_error || "",
    circuit_open_since: raw.circuit_open_since || "",
    egress_ip: raw.egress_ip || "",
    region: raw.region || "",
    last_egress_update: raw.last_egress_update || "",
    last_latency_probe_attempt: raw.last_latency_probe_attempt || "",
    last_authority_latency_probe_attempt: raw.last_authority_latency_probe_attempt || "",
    last_bandwidth_probe_attempt: raw.last_bandwidth_probe_attempt || "",
    last_bandwidth_update: raw.last_bandwidth_update || "",
    last_egress_update_attempt: raw.last_egress_update_attempt || "",
  };

  // Backend uses `omitempty`; field missing means "no reference latency".
  if (typeof reference_latency_ms === "number") {
    normalized.reference_latency_ms = reference_latency_ms;
  }
  if (typeof download_bandwidth_mbps === "number") {
    normalized.download_bandwidth_mbps = download_bandwidth_mbps;
  }

  return normalized;
}

function appendNodeFilters(query: URLSearchParams, filters: NodeListFilters) {
  const appendIfNotEmpty = (key: string, value?: string) => {
    if (!value) {
      return;
    }
    const trimmed = value.trim();
    if (!trimmed) {
      return;
    }
    query.set(key, trimmed);
  };

  appendIfNotEmpty("platform_id", filters.platform_id);
  appendIfNotEmpty("subscription_id", filters.subscription_id);
  appendIfNotEmpty("tag_keyword", filters.tag_keyword);
  appendIfNotEmpty("region", filters.region?.toLowerCase());
  appendIfNotEmpty("egress_ip", filters.egress_ip);
  appendIfNotEmpty("probed_since", filters.probed_since);

  if (filters.circuit_open !== undefined) {
    query.set("circuit_open", String(filters.circuit_open));
  }
  if (filters.has_outbound !== undefined) {
    query.set("has_outbound", String(filters.has_outbound));
  }
  if (filters.enabled !== undefined) {
    query.set("enabled", String(filters.enabled));
  }
}

export async function listNodes(filters: NodeListQuery): Promise<PageResponse<NodeSummary>> {
  const query = new URLSearchParams({
    limit: String(filters.limit ?? 50),
    offset: String(filters.offset ?? 0),
    sort_by: filters.sort_by || "tag",
    sort_order: filters.sort_order || "asc",
  });
  appendNodeFilters(query, filters);

  const data = await apiRequest<PageResponse<ApiNodeSummary>>(`${basePath}?${query.toString()}`);
  return {
    ...data,
    items: data.items.map(normalizeNode),
  };
}

export async function getNode(hash: string): Promise<NodeSummary> {
  const data = await apiRequest<ApiNodeSummary>(`${basePath}/${hash}`);
  return normalizeNode(data);
}

export async function updateNode(hash: string, patch: { manual_disabled: boolean }): Promise<NodeSummary> {
  const data = await apiRequest<ApiNodeSummary>(`${basePath}/${hash}`, {
    method: "PATCH",
    body: patch,
  });
  return normalizeNode(data);
}

export async function probeEgress(hash: string): Promise<EgressProbeResult> {
  return apiRequest<EgressProbeResult>(`${basePath}/${hash}/actions/probe-egress`, {
    method: "POST",
  });
}

export async function probeLatency(hash: string): Promise<LatencyProbeResult> {
  return apiRequest<LatencyProbeResult>(`${basePath}/${hash}/actions/probe-latency`, {
    method: "POST",
  });
}

export async function batchProbeLatency(
  filters: NodeListFilters,
  maxLatencyMs: number
): Promise<BatchLatencyProbeResult> {
  const query = new URLSearchParams();
  appendNodeFilters(query, filters);
  return apiRequest<BatchLatencyProbeResult>(`${basePath}/actions/probe-latency?${query.toString()}`, {
    method: "POST",
    body: { max_latency_ms: maxLatencyMs },
  });
}

export async function probeBandwidth(hash: string): Promise<BandwidthProbeResult> {
  return apiRequest<BandwidthProbeResult>(`${basePath}/${hash}/actions/probe-bandwidth`, {
    method: "POST",
  });
}

export async function batchProbeBandwidth(
  filters: NodeListFilters,
  minDownloadMbps: number
): Promise<BatchBandwidthProbeResult> {
  const query = new URLSearchParams();
  appendNodeFilters(query, filters);
  return apiRequest<BatchBandwidthProbeResult>(`${basePath}/actions/probe-bandwidth?${query.toString()}`, {
    method: "POST",
    body: { min_download_mbps: minDownloadMbps },
  });
}

export async function batchProbeQuality(
  filters: NodeListFilters,
  maxLatencyMs: number,
  minDownloadMbps: number,
  disableFailed: boolean,
  recoverDisabled: boolean
): Promise<BatchQualityProbeResult> {
  const query = new URLSearchParams();
  appendNodeFilters(query, recoverDisabled ? { ...filters, enabled: undefined } : filters);
  return apiRequest<BatchQualityProbeResult>(`${basePath}/actions/probe-quality?${query.toString()}`, {
    method: "POST",
    body: {
      max_latency_ms: maxLatencyMs,
      min_download_mbps: minDownloadMbps,
      disable_failed: disableFailed,
      recover_disabled: recoverDisabled,
    },
  });
}

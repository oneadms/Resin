package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/topology"
)

func newBandwidthLearningTestPool() *topology.GlobalNodePool {
	return topology.NewGlobalNodePool(topology.PoolConfig{
		MaxConsecutiveFailures: func() int { return 3 },
	})
}

func TestRecordBandwidthFromFinishedEvent_RecordsLargeSuccessfulResponse(t *testing.T) {
	pool := newBandwidthLearningTestPool()
	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.30","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub-bandwidth")

	ok := recordBandwidthFromFinishedEvent(pool, proxy.RequestFinishedEvent{
		NodeHash:     hash.Hex(),
		NetOK:        true,
		IngressBytes: 2_000_000,
		DurationNs:   int64(time.Second),
	})
	if !ok {
		t.Fatal("expected real traffic bandwidth sample to be recorded")
	}

	entry, exists := pool.GetEntry(hash)
	if !exists {
		t.Fatal("node missing after bandwidth sample")
	}
	if got := entry.BandwidthMbps(); got != 16 {
		t.Fatalf("bandwidth Mbps = %v, want 16", got)
	}
	if entry.LastBandwidthProbeAttempt.Load() <= 0 || entry.LastBandwidthUpdate.Load() <= 0 {
		t.Fatal("bandwidth timestamps were not updated")
	}
}

func TestRecordBandwidthFromFinishedEvent_IgnoresNoisySamples(t *testing.T) {
	pool := newBandwidthLearningTestPool()
	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.31","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, "sub-bandwidth")

	cases := []proxy.RequestFinishedEvent{
		{NodeHash: hash.Hex(), NetOK: false, IngressBytes: 2_000_000, DurationNs: int64(time.Second)},
		{NodeHash: hash.Hex(), NetOK: true, IsConnect: true, IngressBytes: 2_000_000, DurationNs: int64(time.Second)},
		{NodeHash: hash.Hex(), NetOK: true, IngressBytes: realTrafficBandwidthMinIngressBytes - 1, DurationNs: int64(time.Second)},
		{NodeHash: hash.Hex(), NetOK: true, IngressBytes: 2_000_000, DurationNs: int64(realTrafficBandwidthMinDuration) - 1},
		{NodeHash: "not-a-node-hash", NetOK: true, IngressBytes: 2_000_000, DurationNs: int64(time.Second)},
	}
	for i, ev := range cases {
		if ok := recordBandwidthFromFinishedEvent(pool, ev); ok {
			t.Fatalf("case %d recorded a noisy sample", i)
		}
	}

	entry, exists := pool.GetEntry(hash)
	if !exists {
		t.Fatal("node missing after ignored samples")
	}
	if got := entry.BandwidthMbps(); got != 0 {
		t.Fatalf("bandwidth Mbps = %v, want 0", got)
	}
	if got := entry.LastBandwidthProbeAttempt.Load(); got != 0 {
		t.Fatalf("last bandwidth attempt = %d, want 0", got)
	}
}

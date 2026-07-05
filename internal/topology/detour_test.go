package topology

import (
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/subscription"
)

func newDetourTestPool() (*GlobalNodePool, *subscription.Subscription) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("sub-1", "sub", "https://example.com/sub", true, false)
	subMgr.Register(sub)
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})
	return pool, sub
}

func addDetourTestNode(t *testing.T, pool *GlobalNodePool, sub *subscription.Subscription, tag string, raw string) node.Hash {
	t.Helper()
	hash := node.HashFromRawOptions([]byte(raw))
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{tag}})
	pool.AddNodeFromSub(hash, []byte(raw), sub.ID)
	return hash
}

func TestResolveNodeDetourChain_SingleHop(t *testing.T) {
	pool, sub := newDetourTestPool()
	target := addDetourTestNode(t, pool, sub, "target", `{"type":"direct","tag":"target"}`)
	root := addDetourTestNode(t, pool, sub, "root", `{"type":"direct","tag":"root","detour":"target"}`)

	chain, err := pool.ResolveNodeDetourChain(root)
	if err != nil {
		t.Fatalf("ResolveNodeDetourChain: %v", err)
	}
	if !reflect.DeepEqual(chain, []node.Hash{target}) {
		t.Fatalf("chain = %v, want [%s]", chain, target.Hex())
	}
}

func TestResolveNodeDetourChain_MultipleHops(t *testing.T) {
	pool, sub := newDetourTestPool()
	exit := addDetourTestNode(t, pool, sub, "exit", `{"type":"direct","tag":"exit"}`)
	mid := addDetourTestNode(t, pool, sub, "mid", `{"type":"direct","tag":"mid","detour":"exit"}`)
	root := addDetourTestNode(t, pool, sub, "root", `{"type":"direct","tag":"root","detour":"mid"}`)

	chain, err := pool.ResolveNodeDetourChain(root)
	if err != nil {
		t.Fatalf("ResolveNodeDetourChain: %v", err)
	}
	if !reflect.DeepEqual(chain, []node.Hash{mid, exit}) {
		t.Fatalf("chain = %v, want [%s %s]", chain, mid.Hex(), exit.Hex())
	}
}

func TestResolveNodeDetourChain_MissingTagFails(t *testing.T) {
	pool, sub := newDetourTestPool()
	root := addDetourTestNode(t, pool, sub, "root", `{"type":"direct","tag":"root","detour":"missing"}`)

	_, err := pool.ResolveNodeDetourChain(root)
	if err == nil || !strings.Contains(err.Error(), `detour tag "missing" not found`) {
		t.Fatalf("expected missing-tag error, got %v", err)
	}
}

func TestResolveNodeDetourChain_DuplicateTagFails(t *testing.T) {
	pool, sub := newDetourTestPool()
	addDetourTestNode(t, pool, sub, "dup", `{"type":"direct","tag":"dup-a","server":"1.1.1.1"}`)
	addDetourTestNode(t, pool, sub, "dup", `{"type":"direct","tag":"dup-b","server":"2.2.2.2"}`)
	root := addDetourTestNode(t, pool, sub, "root", `{"type":"direct","tag":"root","detour":"dup"}`)

	_, err := pool.ResolveNodeDetourChain(root)
	if err == nil || !strings.Contains(err.Error(), `detour tag "dup" is ambiguous`) {
		t.Fatalf("expected ambiguous-tag error, got %v", err)
	}
}

func TestResolveNodeDetourChain_CycleFails(t *testing.T) {
	pool, sub := newDetourTestPool()
	a := addDetourTestNode(t, pool, sub, "a", `{"type":"direct","tag":"a","detour":"b"}`)
	addDetourTestNode(t, pool, sub, "b", `{"type":"direct","tag":"b","detour":"a"}`)

	_, err := pool.ResolveNodeDetourChain(a)
	if err == nil || !strings.Contains(err.Error(), "detour cycle") {
		t.Fatalf("expected cycle error, got %v", err)
	}
}

func TestResolveNodeDetourChain_SelfReferenceFails(t *testing.T) {
	pool, sub := newDetourTestPool()
	root := addDetourTestNode(t, pool, sub, "root", `{"type":"direct","tag":"root","detour":"root"}`)

	_, err := pool.ResolveNodeDetourChain(root)
	if err == nil || !strings.Contains(err.Error(), "detour self-reference") {
		t.Fatalf("expected self-reference error, got %v", err)
	}
}

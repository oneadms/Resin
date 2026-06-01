package proxy

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type noopOutbound struct {
	adapter.Outbound
}

func (n *noopOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("not used in transport-pool tests")
}

func (n *noopOutbound) Tag() string  { return "noop" }
func (n *noopOutbound) Type() string { return "noop" }

func TestOutboundTransportPool_ReusesByNodeHash(t *testing.T) {
	pool := newOutboundTransportPool()
	hash := node.Hash{1}

	t1 := pool.Get(hash, &noopOutbound{}, nil)
	t2 := pool.Get(hash, &noopOutbound{}, nil)

	if t1 != t2 {
		t.Fatal("expected same transport instance for identical node hash")
	}
}

func TestOutboundTransportPool_SplitsByNodeHash(t *testing.T) {
	pool := newOutboundTransportPool()
	ob := &noopOutbound{}
	hash1 := node.Hash{1}
	hash2 := node.Hash{2}

	base := pool.Get(hash1, ob, nil)
	byNodeHash := pool.Get(hash2, ob, nil)
	if base == byNodeHash {
		t.Fatal("expected different transport for different node hash")
	}
}

func TestOutboundTransportPool_UsesKeepAliveTransport(t *testing.T) {
	pool := newOutboundTransportPool()
	ob := &noopOutbound{}
	hash := node.Hash{1}

	transport := pool.Get(hash, ob, nil)
	if transport.DisableKeepAlives {
		t.Fatal("expected keep-alive enabled transport")
	}
}

func TestOutboundTransportPool_EvictRemovesNodeTransport(t *testing.T) {
	pool := newOutboundTransportPool()
	hash := node.Hash{1}
	ob := &noopOutbound{}

	t1 := pool.Get(hash, ob, nil)
	pool.Evict(hash)
	t2 := pool.Get(hash, ob, nil)

	if t1 == t2 {
		t.Fatal("expected a new transport after evict")
	}
}

func TestOutboundTransportPool_AppliesConfiguredLimits(t *testing.T) {
	pool := newOutboundTransportPoolWithConfig(OutboundTransportConfig{
		MaxIdleConns:        9,
		MaxIdleConnsPerHost: 3,
		IdleConnTimeout:     12 * time.Second,
	})
	ob := &noopOutbound{}
	hash := node.Hash{1}

	transport := pool.Get(hash, ob, nil)
	if transport.MaxIdleConns != 9 {
		t.Fatalf("MaxIdleConns: got %d, want %d", transport.MaxIdleConns, 9)
	}
	if transport.MaxIdleConnsPerHost != 3 {
		t.Fatalf("MaxIdleConnsPerHost: got %d, want %d", transport.MaxIdleConnsPerHost, 3)
	}
	if transport.IdleConnTimeout != 12*time.Second {
		t.Fatalf("IdleConnTimeout: got %s, want %s", transport.IdleConnTimeout, 12*time.Second)
	}
}

type recordingOutbound struct {
	adapter.Outbound
	networks []string
}

func (r *recordingOutbound) DialContext(_ context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	r.networks = append(r.networks, network)
	return nil, errors.New("dial stopped")
}

func TestOutboundTransportPool_DialNetworkUsesConfig(t *testing.T) {
	ob := &recordingOutbound{}
	pool := newOutboundTransportPoolWithConfig(OutboundTransportConfig{
		DialNetwork: func() string { return "tcp4" },
	})
	transport := pool.Get(node.Hash{1}, ob, nil)

	_, _ = transport.DialContext(context.Background(), "tcp", "example.com:443")

	if len(ob.networks) != 1 || ob.networks[0] != "tcp4" {
		t.Fatalf("dial networks: got %v, want [tcp4]", ob.networks)
	}
}

func TestOutboundTransportPool_DialNetworkReadsLatestValue(t *testing.T) {
	network := "tcp"
	ob := &recordingOutbound{}
	pool := newOutboundTransportPoolWithConfig(OutboundTransportConfig{
		DialNetwork: func() string { return network },
	})
	transport := pool.Get(node.Hash{1}, ob, nil)

	_, _ = transport.DialContext(context.Background(), "tcp", "example.com:443")
	network = "tcp4"
	_, _ = transport.DialContext(context.Background(), "tcp", "example.com:443")

	want := []string{"tcp", "tcp4"}
	if len(ob.networks) != len(want) {
		t.Fatalf("dial networks: got %v, want %v", ob.networks, want)
	}
	for i := range want {
		if ob.networks[i] != want[i] {
			t.Fatalf("dial networks: got %v, want %v", ob.networks, want)
		}
	}
}

func TestOutboundTransportPool_CloseAllClearsEntries(t *testing.T) {
	pool := newOutboundTransportPool()
	ob := &noopOutbound{}

	hashA := node.Hash{1}
	hashB := node.Hash{2}
	t1 := pool.Get(hashA, ob, nil)
	_ = pool.Get(hashB, ob, nil)

	pool.CloseAll()

	t2 := pool.Get(hashA, ob, nil)
	if t1 == t2 {
		t.Fatal("expected a new transport after CloseAll")
	}
}

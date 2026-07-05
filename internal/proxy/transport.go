package proxy

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type OutboundTransportConfig struct {
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
	DialNetwork         func() string
}

const (
	defaultTransportMaxIdleConns        = 1024
	defaultTransportMaxIdleConnsPerHost = 64
	defaultTransportIdleConnTimeout     = 90 * time.Second
)

func normalizeOutboundTransportConfig(cfg OutboundTransportConfig) OutboundTransportConfig {
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = defaultTransportMaxIdleConns
	}
	if cfg.MaxIdleConnsPerHost <= 0 {
		cfg.MaxIdleConnsPerHost = defaultTransportMaxIdleConnsPerHost
	}
	if cfg.IdleConnTimeout <= 0 {
		cfg.IdleConnTimeout = defaultTransportIdleConnTimeout
	}
	return cfg
}

func transportDialNetwork(cfg OutboundTransportConfig, fallback string) string {
	if cfg.DialNetwork == nil {
		return fallback
	}
	if network := cfg.DialNetwork(); network != "" {
		return network
	}
	return fallback
}

// OutboundTransportPool manages reusable outbound HTTP transports keyed by platform and node.
// A single instance should be shared by forward/reverse proxies so keep-alive pools
// are reused and can be evicted on node or platform changes.
type OutboundTransportPool struct {
	config     OutboundTransportConfig
	transports *xsync.Map[outboundTransportKey, *http.Transport]
}

type outboundTransportKey struct {
	PlatformID     string
	NodeHash       node.Hash
	StaticProxyURL string
}

func newOutboundTransportPool() *OutboundTransportPool {
	return NewOutboundTransportPool(OutboundTransportConfig{})
}

func newOutboundTransportPoolWithConfig(cfg OutboundTransportConfig) *OutboundTransportPool {
	return NewOutboundTransportPool(cfg)
}

// NewOutboundTransportPool creates a transport pool with normalized settings.
func NewOutboundTransportPool(cfg OutboundTransportConfig) *OutboundTransportPool {
	return &OutboundTransportPool{
		config:     normalizeOutboundTransportConfig(cfg),
		transports: xsync.NewMap[outboundTransportKey, *http.Transport](),
	}
}

// Get returns a reusable transport for the given platform/node/static-proxy tuple.
func (p *OutboundTransportPool) Get(
	platformID string,
	hash node.Hash,
	staticProxyURL string,
	ob adapter.Outbound,
	sink MetricsEventSink,
) *http.Transport {
	key := outboundTransportKey{
		PlatformID:     platformID,
		NodeHash:       hash,
		StaticProxyURL: staticProxyURL,
	}
	transport, _ := p.transports.LoadOrCompute(key, func() (*http.Transport, bool) {
		return p.newReusableOutboundTransport(ob, staticProxyURL, sink), false
	})
	return transport
}

// Evict closes idle connections for one node's transports and removes them from pool.
func (p *OutboundTransportPool) Evict(hash node.Hash) {
	p.transports.Range(func(key outboundTransportKey, transport *http.Transport) bool {
		if key.NodeHash != hash {
			return true
		}
		if removed, ok := p.transports.LoadAndDelete(key); ok && removed != nil {
			removed.CloseIdleConnections()
		}
		return true
	})
}

// EvictPlatform closes idle connections for one platform's transports.
func (p *OutboundTransportPool) EvictPlatform(platformID string) {
	p.transports.Range(func(key outboundTransportKey, transport *http.Transport) bool {
		if key.PlatformID != platformID {
			return true
		}
		if removed, ok := p.transports.LoadAndDelete(key); ok && removed != nil {
			removed.CloseIdleConnections()
		}
		return true
	})
}

// CloseAll closes idle connections and clears all pooled transports.
func (p *OutboundTransportPool) CloseAll() {
	p.transports.Range(func(_ outboundTransportKey, transport *http.Transport) bool {
		if transport != nil {
			transport.CloseIdleConnections()
		}
		return true
	})
	p.transports.Clear()
}

func (p *OutboundTransportPool) newReusableOutboundTransport(
	ob adapter.Outbound,
	staticProxyURL string,
	sink MetricsEventSink,
) *http.Transport {
	dialContext := newOutboundDialContext(p.config, ob, sink)
	transport := &http.Transport{
		DialContext:         dialContext,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        p.config.MaxIdleConns,
		MaxIdleConnsPerHost: p.config.MaxIdleConnsPerHost,
		IdleConnTimeout:     p.config.IdleConnTimeout,
	}
	if staticProxyURL == "" {
		return transport
	}
	proxyURL, err := parseStaticProxyURL(staticProxyURL)
	if err != nil {
		transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
			return nil, err
		}
		return transport
	}
	switch proxyURL.Scheme {
	case "http", "https":
		transport.Proxy = http.ProxyURL(proxyURL)
	case "socks5", "socks5h":
		transport.DialContext = func(ctx context.Context, _ string, addr string) (net.Conn, error) {
			return dialSOCKS5StaticProxy(ctx, dialContext, proxyURL, addr)
		}
	}
	return transport
}

func newOutboundDialContext(
	cfg OutboundTransportConfig,
	ob adapter.Outbound,
	sink MetricsEventSink,
) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialNetwork := transportDialNetwork(cfg, network)
		conn, err := ob.DialContext(ctx, dialNetwork, M.ParseSocksaddr(addr))
		if err != nil {
			return nil, err
		}
		if sink != nil {
			sink.OnConnectionLifecycle(ConnectionOutbound, ConnectionOpen)
			conn = newCountingConn(conn, sink)
		}
		return conn, nil
	}
}

func newDirectHTTPTransport(cfg OutboundTransportConfig, sink MetricsEventSink) *http.Transport {
	cfg = normalizeOutboundTransportConfig(cfg)
	dialer := &net.Dialer{}
	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialNetwork := transportDialNetwork(cfg, network)
			conn, err := dialer.DialContext(ctx, dialNetwork, addr)
			if err != nil {
				return nil, err
			}
			if sink != nil {
				sink.OnConnectionLifecycle(ConnectionOutbound, ConnectionOpen)
				conn = newCountingConn(conn, sink)
			}
			return conn, nil
		},
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:     cfg.IdleConnTimeout,
	}
}

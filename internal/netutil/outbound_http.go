package netutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

const defaultOutboundUserAgent = "Resin/1.0"

type ConnLifecycleOp uint8

const (
	ConnLifecycleOpen ConnLifecycleOp = iota
	ConnLifecycleClose
)

// OutboundHTTPOptions controls outbound-backed HTTP execution behavior.
type OutboundHTTPOptions struct {
	// RequireStatusOK enforces HTTP 200 status; otherwise any status is accepted.
	RequireStatusOK bool
	// UserAgent overrides the request User-Agent when non-empty.
	UserAgent string
	// OnConnLifecycle is called with open/close lifecycle events to track connection
	// lifecycle for metrics. Set by probe callers to count outbound connections;
	// left nil for download callers (GeoIP, subscription) to exclude from stats.
	OnConnLifecycle func(op ConnLifecycleOp)
}

// HTTPGetViaOutbound executes an HTTP GET through the provided outbound.
// Timeout and cancellation are controlled solely by ctx.
func HTTPGetViaOutbound(
	ctx context.Context,
	outbound adapter.Outbound,
	url string,
	opts OutboundHTTPOptions,
) ([]byte, time.Duration, error) {
	if outbound == nil {
		return nil, 0, fmt.Errorf("outbound fetch: outbound is nil")
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := outbound.DialContext(ctx, network, M.ParseSocksaddr(addr))
			if err != nil {
				return nil, err
			}
			if opts.OnConnLifecycle != nil {
				opts.OnConnLifecycle(ConnLifecycleOpen)
				return &connCloseHook{Conn: conn, onClose: func() { opts.OnConnLifecycle(ConnLifecycleClose) }}, nil
			}
			return conn, nil
		},
		DisableKeepAlives: true,
		ForceAttemptHTTP2: true,
	}

	client := &http.Client{
		Transport: transport,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}

	userAgent := opts.UserAgent
	if userAgent == "" {
		userAgent = defaultOutboundUserAgent
	}
	req.Header.Set("User-Agent", userAgent)

	var start time.Time
	var latency time.Duration
	trace := &httptrace.ClientTrace{
		TLSHandshakeStart: func() { start = time.Now() },
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			if err == nil {
				latency = time.Since(start)
			}
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(ctx, trace))

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if opts.RequireStatusOK && resp.StatusCode != http.StatusOK {
		return nil, latency, fmt.Errorf("outbound fetch: unexpected status %d from %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, latency, err
	}

	return body, latency, nil
}

// HTTPDownloadViaOutbound streams up to maxBytes through the provided outbound
// and returns the downloaded byte count and response-body transfer duration.
func HTTPDownloadViaOutbound(
	ctx context.Context,
	outbound adapter.Outbound,
	url string,
	maxBytes int64,
	opts OutboundHTTPOptions,
) (int64, time.Duration, error) {
	if outbound == nil {
		return 0, 0, fmt.Errorf("outbound download: outbound is nil")
	}
	if maxBytes <= 0 {
		return 0, 0, fmt.Errorf("outbound download: maxBytes must be positive")
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := outbound.DialContext(ctx, network, M.ParseSocksaddr(addr))
			if err != nil {
				return nil, err
			}
			if opts.OnConnLifecycle != nil {
				opts.OnConnLifecycle(ConnLifecycleOpen)
				return &connCloseHook{Conn: conn, onClose: func() { opts.OnConnLifecycle(ConnLifecycleClose) }}, nil
			}
			return conn, nil
		},
		DisableKeepAlives: true,
		ForceAttemptHTTP2: true,
	}
	client := &http.Client{Transport: transport}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, 0, err
	}
	userAgent := opts.UserAgent
	if userAgent == "" {
		userAgent = defaultOutboundUserAgent
	}
	req.Header.Set("User-Agent", userAgent)

	requestStart := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, time.Since(requestStart), err
	}
	defer resp.Body.Close()

	if opts.RequireStatusOK && resp.StatusCode != http.StatusOK {
		return 0, time.Since(requestStart), fmt.Errorf("outbound download: unexpected status %d from %s", resp.StatusCode, url)
	}

	transferStart := time.Now()
	downloaded, err := io.Copy(io.Discard, io.LimitReader(resp.Body, maxBytes))
	elapsed := time.Since(transferStart)
	if err != nil {
		return downloaded, elapsed, err
	}
	if downloaded == 0 {
		return 0, elapsed, fmt.Errorf("outbound download: empty response from %s", url)
	}
	return downloaded, elapsed, nil
}

// HTTPUploadViaOutbound streams maxBytes of request body data through the
// provided outbound and returns the uploaded byte count and request duration.
func HTTPUploadViaOutbound(
	ctx context.Context,
	outbound adapter.Outbound,
	url string,
	maxBytes int64,
	opts OutboundHTTPOptions,
) (int64, time.Duration, error) {
	if outbound == nil {
		return 0, 0, fmt.Errorf("outbound upload: outbound is nil")
	}
	if maxBytes <= 0 {
		return 0, 0, fmt.Errorf("outbound upload: maxBytes must be positive")
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := outbound.DialContext(ctx, network, M.ParseSocksaddr(addr))
			if err != nil {
				return nil, err
			}
			if opts.OnConnLifecycle != nil {
				opts.OnConnLifecycle(ConnLifecycleOpen)
				return &connCloseHook{Conn: conn, onClose: func() { opts.OnConnLifecycle(ConnLifecycleClose) }}, nil
			}
			return conn, nil
		},
		DisableKeepAlives: true,
		ForceAttemptHTTP2: true,
	}
	client := &http.Client{Transport: transport}

	body := &countingPatternReader{remaining: maxBytes}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, io.NopCloser(body))
	if err != nil {
		return 0, 0, err
	}
	req.ContentLength = maxBytes
	req.Header.Set("Content-Type", "application/octet-stream")
	userAgent := opts.UserAgent
	if userAgent == "" {
		userAgent = defaultOutboundUserAgent
	}
	req.Header.Set("User-Agent", userAgent)

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)
	uploaded := body.Total()
	if err != nil {
		return uploaded, elapsed, err
	}
	defer resp.Body.Close()

	if opts.RequireStatusOK && resp.StatusCode != http.StatusOK {
		return uploaded, elapsed, fmt.Errorf("outbound upload: unexpected status %d from %s", resp.StatusCode, url)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	if uploaded == 0 {
		return 0, elapsed, fmt.Errorf("outbound upload: empty request body sent to %s", url)
	}
	return uploaded, elapsed, nil
}

type countingPatternReader struct {
	remaining int64
	total     int64
}

func (r *countingPatternReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	for i := range p {
		p[i] = byte(i)
	}
	n := len(p)
	r.remaining -= int64(n)
	r.total += int64(n)
	return n, nil
}

func (r *countingPatternReader) Total() int64 {
	if r == nil {
		return 0
	}
	return r.total
}

// connCloseHook wraps a net.Conn and calls onClose exactly once on Close.
type connCloseHook struct {
	net.Conn
	onClose   func()
	closeOnce sync.Once
	closeErr  error
}

func (c *connCloseHook) Close() error {
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
		c.closeErr = c.Conn.Close()
	})
	return c.closeErr
}

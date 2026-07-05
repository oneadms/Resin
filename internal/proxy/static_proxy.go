package proxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

type dialContextFunc func(context.Context, string, string) (net.Conn, error)

func parseStaticProxyURL(raw string) (*url.URL, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, nil
	}
	u, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("static_proxy_url: invalid URL: %w", err)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	switch u.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return nil, fmt.Errorf("static_proxy_url: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" || u.Hostname() == "" {
		return nil, fmt.Errorf("static_proxy_url: host is required")
	}
	return u, nil
}

func dialRoutedTarget(
	ctx context.Context,
	cfg OutboundTransportConfig,
	ob adapter.Outbound,
	target string,
	staticProxyURL string,
	sink MetricsEventSink,
) (net.Conn, error) {
	dialContext := newOutboundDialContext(cfg, ob, sink)
	proxyURL, err := parseStaticProxyURL(staticProxyURL)
	if err != nil {
		return nil, err
	}
	if proxyURL == nil {
		return dialContext(ctx, "tcp", target)
	}
	switch proxyURL.Scheme {
	case "http", "https":
		return dialHTTPStaticProxy(ctx, dialContext, proxyURL, target)
	case "socks5", "socks5h":
		return dialSOCKS5StaticProxy(ctx, dialContext, proxyURL, target)
	default:
		return nil, fmt.Errorf("static_proxy_url: unsupported scheme %q", proxyURL.Scheme)
	}
}

func staticProxyAddress(u *url.URL) (string, error) {
	if u == nil || u.Hostname() == "" {
		return "", fmt.Errorf("static_proxy_url: host is required")
	}
	if u.Port() != "" {
		return u.Host, nil
	}
	switch u.Scheme {
	case "http":
		return net.JoinHostPort(u.Hostname(), "80"), nil
	case "https":
		return net.JoinHostPort(u.Hostname(), "443"), nil
	case "socks5", "socks5h":
		return net.JoinHostPort(u.Hostname(), "1080"), nil
	default:
		return "", fmt.Errorf("static_proxy_url: unsupported scheme %q", u.Scheme)
	}
}

func dialHTTPStaticProxy(ctx context.Context, dialContext dialContextFunc, proxyURL *url.URL, target string) (net.Conn, error) {
	proxyAddr, err := staticProxyAddress(proxyURL)
	if err != nil {
		return nil, err
	}
	conn, err := dialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, err
	}
	if proxyURL.Scheme == "https" {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: proxyURL.Hostname()})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		conn = tlsConn
	}

	clearDeadline := setConnDeadlineFromContext(conn, ctx)
	defer clearDeadline()

	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: target},
		Host:   target,
		Header: make(http.Header),
	}
	if auth := staticProxyAuthorization(proxyURL); auth != "" {
		req.Header.Set("Proxy-Authorization", auth)
	}
	if err := req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = conn.Close()
		return nil, fmt.Errorf("static proxy CONNECT failed: %s", resp.Status)
	}
	if reader.Buffered() > 0 {
		return &prefetchedConn{Conn: conn, reader: reader}, nil
	}
	return conn, nil
}

func staticProxyAuthorization(proxyURL *url.URL) string {
	if proxyURL == nil || proxyURL.User == nil {
		return ""
	}
	username := proxyURL.User.Username()
	password, hasPassword := proxyURL.User.Password()
	if username == "" && !hasPassword {
		return ""
	}
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

type prefetchedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *prefetchedConn) Read(p []byte) (int, error) {
	if c.reader != nil && c.reader.Buffered() > 0 {
		return c.reader.Read(p)
	}
	return c.Conn.Read(p)
}

func dialSOCKS5StaticProxy(ctx context.Context, dialContext dialContextFunc, proxyURL *url.URL, target string) (net.Conn, error) {
	proxyAddr, err := staticProxyAddress(proxyURL)
	if err != nil {
		return nil, err
	}
	conn, err := dialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, err
	}

	clearDeadline := setConnDeadlineFromContext(conn, ctx)
	defer clearDeadline()

	if err := socks5Handshake(conn, proxyURL); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := socks5Connect(ctx, conn, proxyURL.Scheme, target); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func socks5Handshake(conn net.Conn, proxyURL *url.URL) error {
	methods := []byte{0x00}
	username, password, hasAuth, err := staticProxyUserPass(proxyURL)
	if err != nil {
		return err
	}
	if hasAuth {
		methods = append(methods, 0x02)
	}
	req := append([]byte{0x05, byte(len(methods))}, methods...)
	if _, err := conn.Write(req); err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[0] != 0x05 {
		return fmt.Errorf("static socks5 proxy: unexpected version %d", resp[0])
	}
	switch resp[1] {
	case 0x00:
		return nil
	case 0x02:
		if !hasAuth {
			return fmt.Errorf("static socks5 proxy: username/password required")
		}
		return socks5UserPassAuth(conn, username, password)
	case 0xff:
		return fmt.Errorf("static socks5 proxy: no acceptable auth method")
	default:
		return fmt.Errorf("static socks5 proxy: unsupported auth method %d", resp[1])
	}
}

func staticProxyUserPass(proxyURL *url.URL) (string, string, bool, error) {
	if proxyURL == nil || proxyURL.User == nil {
		return "", "", false, nil
	}
	username := proxyURL.User.Username()
	password, hasPassword := proxyURL.User.Password()
	hasAuth := username != "" || hasPassword
	if !hasAuth {
		return "", "", false, nil
	}
	if len(username) > 255 || len(password) > 255 {
		return "", "", false, fmt.Errorf("static socks5 proxy: username/password must be <= 255 bytes")
	}
	return username, password, true, nil
}

func socks5UserPassAuth(conn net.Conn, username, password string) error {
	req := []byte{0x01, byte(len(username))}
	req = append(req, []byte(username)...)
	req = append(req, byte(len(password)))
	req = append(req, []byte(password)...)
	if _, err := conn.Write(req); err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[0] != 0x01 || resp[1] != 0x00 {
		return fmt.Errorf("static socks5 proxy: username/password auth failed")
	}
	return nil
}

func socks5Connect(ctx context.Context, conn net.Conn, scheme string, target string) error {
	addr, err := socks5TargetAddress(ctx, scheme, target)
	if err != nil {
		return err
	}
	req := []byte{0x05, 0x01, 0x00, addr.atyp}
	req = append(req, addr.host...)
	req = append(req, byte(addr.port>>8), byte(addr.port))
	if _, err := conn.Write(req); err != nil {
		return err
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}
	if header[0] != 0x05 {
		return fmt.Errorf("static socks5 proxy: unexpected version %d", header[0])
	}
	if header[1] != 0x00 {
		return fmt.Errorf("static socks5 proxy connect failed: %s", socks5ReplyError(header[1]))
	}
	if err := discardSOCKS5BindAddress(conn, header[3]); err != nil {
		return err
	}
	return nil
}

type socks5Address struct {
	atyp byte
	host []byte
	port uint16
}

func socks5TargetAddress(ctx context.Context, scheme string, target string) (socks5Address, error) {
	host, portRaw, err := net.SplitHostPort(target)
	if err != nil {
		return socks5Address{}, fmt.Errorf("static socks5 proxy: target must be host:port: %w", err)
	}
	port64, err := strconv.ParseUint(portRaw, 10, 16)
	if err != nil || port64 == 0 {
		return socks5Address{}, fmt.Errorf("static socks5 proxy: invalid target port %q", portRaw)
	}
	port := uint16(port64)

	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return socks5Address{atyp: 0x01, host: v4, port: port}, nil
		}
		return socks5Address{atyp: 0x04, host: ip.To16(), port: port}, nil
	}

	if scheme == "socks5" {
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return socks5Address{}, err
		}
		for _, ip := range ips {
			if v4 := ip.IP.To4(); v4 != nil {
				return socks5Address{atyp: 0x01, host: v4, port: port}, nil
			}
		}
		if len(ips) > 0 {
			return socks5Address{atyp: 0x04, host: ips[0].IP.To16(), port: port}, nil
		}
		return socks5Address{}, fmt.Errorf("static socks5 proxy: no IPs for %s", host)
	}

	if len(host) > 255 {
		return socks5Address{}, fmt.Errorf("static socks5 proxy: target host too long")
	}
	return socks5Address{atyp: 0x03, host: append([]byte{byte(len(host))}, []byte(host)...), port: port}, nil
}

func discardSOCKS5BindAddress(conn net.Conn, atyp byte) error {
	var n int
	switch atyp {
	case 0x01:
		n = 4
	case 0x03:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return err
		}
		n = int(lenBuf[0])
	case 0x04:
		n = 16
	default:
		return fmt.Errorf("static socks5 proxy: unsupported bind address type %d", atyp)
	}
	if n > 0 {
		if _, err := io.CopyN(io.Discard, conn, int64(n)); err != nil {
			return err
		}
	}
	if _, err := io.CopyN(io.Discard, conn, 2); err != nil {
		return err
	}
	return nil
}

func socks5ReplyError(code byte) string {
	switch code {
	case 0x01:
		return "general failure"
	case 0x02:
		return "connection not allowed"
	case 0x03:
		return "network unreachable"
	case 0x04:
		return "host unreachable"
	case 0x05:
		return "connection refused"
	case 0x06:
		return "TTL expired"
	case 0x07:
		return "command not supported"
	case 0x08:
		return "address type not supported"
	default:
		return fmt.Sprintf("reply code %d", code)
	}
}

func setConnDeadlineFromContext(conn net.Conn, ctx context.Context) func() {
	if conn == nil || ctx == nil {
		return func() {}
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		return func() { _ = conn.SetDeadline(time.Time{}) }
	}
	return func() {}
}

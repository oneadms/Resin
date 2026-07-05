package proxy

import (
	"bufio"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
)

func TestDialHTTPStaticProxyConnectsThroughProxy(t *testing.T) {
	proxyURL, err := parseStaticProxyURL("http://user:pass@proxy.example:8080")
	if err != nil {
		t.Fatalf("parseStaticProxyURL: %v", err)
	}

	dialed := make(chan string, 1)
	proxyErr := make(chan error, 1)
	dialContext := func(_ context.Context, _ string, addr string) (net.Conn, error) {
		client, server := net.Pipe()
		dialed <- addr
		go func() {
			defer server.Close()
			req, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				proxyErr <- err
				return
			}
			if req.Method != http.MethodConnect || req.Host != "target.example:443" {
				proxyErr <- errUnexpectedProxyRequest(req.Method, req.Host)
				return
			}
			wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
			if got := req.Header.Get("Proxy-Authorization"); got != wantAuth {
				proxyErr <- errUnexpectedProxyAuth(got, wantAuth)
				return
			}
			_, _ = io.WriteString(server, "HTTP/1.1 200 Connection Established\r\n\r\n")
			proxyErr <- nil
		}()
		return client, nil
	}

	conn, err := dialHTTPStaticProxy(context.Background(), dialContext, proxyURL, "target.example:443")
	if err != nil {
		t.Fatalf("dialHTTPStaticProxy: %v", err)
	}
	defer conn.Close()
	if got := <-dialed; got != "proxy.example:8080" {
		t.Fatalf("dialed addr = %q, want proxy.example:8080", got)
	}
	if err := <-proxyErr; err != nil {
		t.Fatalf("proxy observed error: %v", err)
	}
}

func TestDialSOCKS5HStaticProxyConnectsThroughProxy(t *testing.T) {
	proxyURL, err := url.Parse("socks5h://proxy.example:1080")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}

	dialed := make(chan string, 1)
	proxyErr := make(chan error, 1)
	dialContext := func(_ context.Context, _ string, addr string) (net.Conn, error) {
		client, server := net.Pipe()
		dialed <- addr
		go func() {
			defer server.Close()
			if err := expectSOCKS5Connect(server, "target.example", 443); err != nil {
				proxyErr <- err
				return
			}
			proxyErr <- nil
		}()
		return client, nil
	}

	conn, err := dialSOCKS5StaticProxy(context.Background(), dialContext, proxyURL, "target.example:443")
	if err != nil {
		t.Fatalf("dialSOCKS5StaticProxy: %v", err)
	}
	defer conn.Close()
	if got := <-dialed; got != "proxy.example:1080" {
		t.Fatalf("dialed addr = %q, want proxy.example:1080", got)
	}
	if err := <-proxyErr; err != nil {
		t.Fatalf("proxy observed error: %v", err)
	}
}

func expectSOCKS5Connect(conn net.Conn, wantHost string, wantPort uint16) error {
	hello := make([]byte, 3)
	if _, err := io.ReadFull(conn, hello); err != nil {
		return err
	}
	if hello[0] != 0x05 || hello[1] != 0x01 || hello[2] != 0x00 {
		return errUnexpectedSOCKSBytes("hello", hello)
	}
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
		return err
	}

	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}
	if header[0] != 0x05 || header[1] != 0x01 || header[2] != 0x00 || header[3] != 0x03 {
		return errUnexpectedSOCKSBytes("connect header", header[:4])
	}
	hostLen := int(header[4])
	host := make([]byte, hostLen)
	if _, err := io.ReadFull(conn, host); err != nil {
		return err
	}
	port := make([]byte, 2)
	if _, err := io.ReadFull(conn, port); err != nil {
		return err
	}
	gotPort := uint16(port[0])<<8 | uint16(port[1])
	if string(host) != wantHost || gotPort != wantPort {
		return errUnexpectedSOCKSTarget(string(host), gotPort, wantHost, wantPort)
	}
	_, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0x1f, 0x90})
	return err
}

type staticProxyTestError string

func (e staticProxyTestError) Error() string { return string(e) }

func errUnexpectedProxyRequest(method, host string) error {
	return staticProxyTestError("unexpected proxy request: " + method + " " + host)
}

func errUnexpectedProxyAuth(got, want string) error {
	return staticProxyTestError("unexpected proxy auth: got " + got + " want " + want)
}

func errUnexpectedSOCKSBytes(stage string, got []byte) error {
	return staticProxyTestError("unexpected socks " + stage + " bytes")
}

func errUnexpectedSOCKSTarget(gotHost string, gotPort uint16, wantHost string, wantPort uint16) error {
	return staticProxyTestError("unexpected socks target: got " + gotHost + " want " + wantHost)
}

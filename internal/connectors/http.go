// Package connectors reaches the remote MCP servers a user connects: it finds
// how a server signs in, registers the harness with its authorization
// server, trades the user's consent for tokens, and opens the sessions a run
// calls the server's tools through. A connector's address is the user's to
// choose, so every request leaves through a client that reaches only public
// addresses over HTTPS.
package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"code.dny.dev/ssrf"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/pages"
)

const (
	httpsPort = 443
	// maxURLBytes bounds a server's address and every address its metadata
	// names.
	maxURLBytes = 2048
	// maxMetadataBytes bounds a metadata document or registration answer.
	maxMetadataBytes = 1 << 20
	// maxResponseBytes bounds any one response a server sends, a tool's
	// result included.
	maxResponseBytes = 16 << 20
	maxRedirects     = 3
	dialTimeout      = 10 * time.Second
	headerTimeout    = time.Minute
	idleTimeout      = 90 * time.Second
	maxIdleConns     = 64
	maxHeaderBytes   = 64 << 10
	// requestTimeout bounds one request of finding or signing in to a
	// server.
	requestTimeout = 20 * time.Second
	userAgent      = "TinfoilChat/1.0 (+https://tinfoil.sh)"
)

// errNotPublic refuses an address that is not a public HTTPS one.
var errNotPublic = errors.New("connectors need a public HTTPS address")

// ServerURL is a server's address as connectors keep it: HTTPS on its
// standard port, at a host name rather than an address, without credentials
// or a fragment.
func ServerURL(raw string) (string, error) {
	if len(raw) > maxURLBytes {
		return "", errors.New("the address is too long")
	}
	normalized, err := pages.NormalizeURL(raw)
	if err != nil {
		return "", err
	}
	parsed, err := url.Parse(normalized)
	if err != nil || parsed.Scheme != "https" || parsed.Port() != "" && parsed.Port() != strconv.Itoa(httpsPort) {
		return "", errNotPublic
	}
	return normalized, nil
}

// transport reaches public addresses alone, checked as each connection
// dials, so a name that resolves to a private address, or a redirect to
// one, is refused.
func transport() http.RoundTripper {
	guard := ssrf.New(ssrf.WithPorts(httpsPort))
	dialer := &net.Dialer{Timeout: dialTimeout, Control: guard.Safe}
	return &http.Transport{
		DialContext:            dialer.DialContext,
		ForceAttemptHTTP2:      true,
		TLSHandshakeTimeout:    dialTimeout,
		ResponseHeaderTimeout:  headerTimeout,
		IdleConnTimeout:        idleTimeout,
		MaxIdleConns:           maxIdleConns,
		MaxResponseHeaderBytes: maxHeaderBytes,
	}
}

// bounded caps every response body, so a server cannot fill the enclave's
// memory with one.
type bounded struct{ inner http.RoundTripper }

func (b bounded) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("User-Agent", userAgent)
	resp, err := b.inner.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	resp.Body = &cappedBody{ReadCloser: resp.Body, left: maxResponseBytes}
	return resp, nil
}

type cappedBody struct {
	io.ReadCloser
	left int64
}

func (c *cappedBody) Read(p []byte) (int, error) {
	if c.left <= 0 {
		return 0, errors.New("the server's response is too large")
	}
	if int64(len(p)) > c.left {
		p = p[:c.left]
	}
	n, err := c.ReadCloser.Read(p)
	c.left -= int64(n)
	return n, err
}

// clients are the two ways the package reaches a server: fetch follows a
// few redirects between public HTTPS addresses, for metadata documents;
// direct follows none, for every request that carries a credential, which a
// redirect would hand to another host.
type clients struct {
	fetch, direct *http.Client
}

func newClients(inner http.RoundTripper) clients {
	rt := bounded{inner: inner}
	return clients{
		fetch: &http.Client{Transport: rt, CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > maxRedirects {
				return errors.New("too many redirects")
			}
			if _, err := ServerURL(req.URL.String()); err != nil {
				return err
			}
			return nil
		}},
		direct: &http.Client{Transport: rt, CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("the server redirected a request that carries a credential")
		}},
	}
}

// fetchJSON reads the JSON document at address into out, reporting whether
// the server had one there.
func (c *Client) fetchJSON(ctx context.Context, address string, out any) (bool, error) {
	if _, err := ServerURL(address); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.fetch.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusGone {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("the server answered %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMetadataBytes+1))
	if err != nil {
		return false, err
	}
	if len(body) > maxMetadataBytes {
		return false, errors.New("the server's metadata is too large")
	}
	if err := json.Unmarshal(body, out); err != nil {
		return false, nil
	}
	return true, nil
}

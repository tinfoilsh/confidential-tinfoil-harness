package pages

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/context-dot-dev/context-go-sdk/option"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

const testKey = "test-api-key"

// scrapeAnswer is context.dev's answer for a page of the given type and
// metadata.
func scrapeAnswer(pageType string, metadata map[string]any) map[string]any {
	return map[string]any{"success": true, "html": "<html></html>", "url": "https://example.com/article", "type": pageType, "metadata": metadata,
		"cache_metadata": map[string]any{"status": "zdr", "age_ms": 0}}
}

// testReader reads pages from a context.dev that answers as contextDev does,
// and favicons from icons.
func testReader(t *testing.T, contextDev, icons http.HandlerFunc) *Reader {
	t.Helper()
	pages := httptest.NewServer(contextDev)
	t.Cleanup(pages.Close)
	iconServer := httptest.NewServer(icons)
	t.Cleanup(iconServer.Close)
	return newReader(testKey, iconServer.URL, option.WithBaseURL(pages.URL), option.WithMaxRetries(0))
}

func answer(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

// refusalOf is the protocol error err is, or nil.
func refusalOf(err error) *api.Error {
	var refusal *api.Error
	if errors.As(err, &refusal) {
		return refusal
	}
	return nil
}

func unused(t *testing.T) http.HandlerFunc {
	return func(http.ResponseWriter, *http.Request) { t.Error("an unexpected request was made") }
}

func TestNormalizeURLRejectsUnsafeTargets(t *testing.T) {
	for _, rawURL := range []string{
		"", "not a url", "ftp://example.com", "http://user:pass@example.com", "http://localhost", "http://foo.local",
		"http://foo.internal", "http://127.0.0.1", "http://[::1]", "http://10.0.0.1", "http://169.254.169.254/latest/meta-data/",
		"http://192.168.1.1", "http://[fe80::1]", "http://example.com:9200",
		"http://127.1/", "http://0x7f.0.0.1/", "http://2130706433/", "http://0/", "http://0x/", "http://10.0.0.1./",
		"http://localhost./", "http://foo.internal./",
	} {
		if _, err := NormalizeURL(rawURL); err == nil {
			t.Errorf("%q was accepted", rawURL)
		}
	}
	if _, err := NormalizeURL("http://secret-host.internal/x"); err == nil || strings.Contains(err.Error(), "secret-host") {
		t.Fatalf("a refusal named the URL: %v", err)
	}
}

func TestNormalizeURLCanonicalizesAPublicURL(t *testing.T) {
	normalized, err := NormalizeURL("  https://EXAMPLE.com/article?ref=one#section  ")
	if err != nil || normalized != "https://example.com/article?ref=one" {
		t.Fatalf("normalized %q, error %v", normalized, err)
	}
	// Numbers name a host only in its last label.
	for _, public := range []string{"https://1password.com", "https://123.example.com", "https://example.com./", "https://example.0xyz"} {
		if _, err := NormalizeURL(public); err != nil {
			t.Errorf("%q was refused: %v", public, err)
		}
	}
}

func TestDetailsAreReadFreshWithoutRetention(t *testing.T) {
	reader := testReader(t, func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if r.Header.Get("Authorization") != "Bearer "+testKey || !strings.HasSuffix(r.URL.Path, "/web/scrape/html") ||
			query.Get("url") != "https://example.com/article" || query.Get("maxAgeMs") != "0" || query.Get("zdr") != "enabled" {
			t.Errorf("context.dev was asked %s %s with %q", r.Method, r.URL, r.Header.Get("Authorization"))
		}
		answer(t, w, scrapeAnswer("html", map[string]any{"sourceUrl": "https://example.com/article", "finalUrl": "https://example.com/posts/article",
			"title": " Example Article ", "description": "", "siteName": "Example", "image": "/images/cover.jpg"}))
	}, unused(t))
	details, err := reader.Details(context.Background(), "https://EXAMPLE.com/article#top")
	if err != nil {
		t.Fatal(err)
	}
	if details.URL != "https://example.com/posts/article" || *details.Title != "Example Article" || details.Description != nil ||
		*details.SiteName != "Example" || *details.Image != "https://example.com/images/cover.jpg" {
		t.Fatalf("details %+v", details)
	}
}

func TestDetailsOfAPageThatIsNotHTMLAreRefused(t *testing.T) {
	reader := testReader(t, func(w http.ResponseWriter, _ *http.Request) {
		answer(t, w, scrapeAnswer("json", map[string]any{"sourceUrl": "https://example.com/data", "title": "Data"}))
	}, unused(t))
	if _, err := reader.Details(context.Background(), "https://example.com/data"); refusalOf(err) == nil || refusalOf(err).Code != "UPSTREAM_REFUSED" {
		t.Fatalf("a JSON page read as %v", err)
	}
}

func TestDetailsOfAURLThatIsNotAllowedAskNobody(t *testing.T) {
	reader := testReader(t, unused(t), unused(t))
	for _, read := range []func(context.Context, string) error{
		func(ctx context.Context, u string) error { _, err := reader.Details(ctx, u); return err },
		func(ctx context.Context, u string) error { _, err := reader.Favicon(ctx, u); return err },
	} {
		if err := read(context.Background(), "http://169.254.169.254/latest/meta-data/"); refusalOf(err) == nil || refusalOf(err).HTTP != 400 {
			t.Fatalf("a link-local address was read: %v", err)
		}
	}
}

func TestAContextDevFailureNamesNoURL(t *testing.T) {
	reader := testReader(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, `{"message":"ZDR not enabled","error_code":"FORBIDDEN"}`)
	}, unused(t))
	_, err := reader.contextDev.scrape(context.Background(), "https://private.example/secret-path", 1024)
	if err == nil || !strings.Contains(err.Error(), "403") || strings.Contains(err.Error(), "secret-path") {
		t.Fatalf("the failure read %v", err)
	}
	unreachable := newContextDev(testKey, fetchTimeout, option.WithBaseURL("http://127.0.0.1:1"), option.WithMaxRetries(0))
	if _, err := unreachable.scrape(context.Background(), "https://private.example/secret-path", 1024); err == nil || strings.Contains(err.Error(), "secret-path") {
		t.Fatalf("a transport failure read %v", err)
	}
}

func TestAContextDevAnswerPastTheLimitIsRefused(t *testing.T) {
	const maxBodyBytes = 4
	reader := testReader(t, func(w http.ResponseWriter, _ *http.Request) {
		page := scrapeAnswer("html", map[string]any{"sourceUrl": "https://example.com"})
		page["html"] = strings.Repeat("x", maxBodyBytes*apiResponseSizeMultiplier+maxAPIResponseOverhead+1024)
		answer(t, w, page)
	}, unused(t))
	if _, err := reader.contextDev.scrape(context.Background(), "https://example.com", maxBodyBytes); err == nil {
		t.Fatal("an oversized answer was read")
	}
	if _, err := reader.contextDev.scrape(context.Background(), "https://example.com", maxSupportedBodyBytes+1); err == nil {
		t.Fatal("a limit that would overflow was accepted")
	}
}

func TestALimitedBodyReadsUpToItsLimit(t *testing.T) {
	const maxBytes = 4
	at := &limitedReadCloser{inner: io.NopCloser(strings.NewReader("xxxx")), remaining: maxBytes + 1}
	if read, err := io.ReadAll(at); err != nil || len(read) != maxBytes {
		t.Fatalf("a body at the limit read %d bytes, error %v", len(read), err)
	}
	past := &limitedReadCloser{inner: io.NopCloser(strings.NewReader("xxxxx")), remaining: maxBytes + 1}
	if _, err := io.ReadAll(past); err == nil {
		t.Fatal("a body past the limit was read")
	}
}

func TestAFaviconIsAskedOfDuckDuckGoByHostOnly(t *testing.T) {
	icon := []byte("\x00\x00\x01\x00icon")
	reader := testReader(t, unused(t), func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ip3/example.com.ico" || r.URL.RawQuery != "" {
			t.Errorf("DuckDuckGo was asked for %s", r.URL)
		}
		w.Header().Set("Content-Type", "image/x-icon")
		w.Write(icon)
	})
	favicon, err := reader.Favicon(context.Background(), "https://example.com/private/page?q=secret")
	if err != nil || favicon.Status != FaviconFound || string(favicon.Bytes) != string(icon) || favicon.ContentType != "image/x-icon" {
		t.Fatalf("favicon %+v, error %v", favicon, err)
	}
}

func TestAFaviconWithoutAnImageTypeIsDetected(t *testing.T) {
	png := []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")
	reader := testReader(t, unused(t), func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(png)
	})
	if favicon, err := reader.Favicon(context.Background(), "https://example.com"); err != nil || favicon.ContentType != "image/png" {
		t.Fatalf("favicon %+v, error %v", favicon, err)
	}
}

// A site without a favicon answers as the protocol says, with empty bytes
// rather than none.
func TestASiteWithoutAFaviconIsMissing(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusGone} {
		reader := testReader(t, unused(t), func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) })
		favicon, err := reader.Favicon(context.Background(), "https://example.com")
		if err != nil {
			t.Fatal(err)
		}
		if sent := string(api.Raw(favicon)); sent != `{"status":"missing","favicon_bytes":"","favicon_content_type":""}` {
			t.Fatalf("HTTP %d sent %s", status, sent)
		}
	}
}

func TestAFaviconDuckDuckGoCannotServeNowIsAskedForAgainLater(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		reader := testReader(t, unused(t), func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(status)
		})
		_, err := reader.Favicon(context.Background(), "https://example.com")
		if refusal := refusalOf(err); refusal == nil || refusal.HTTP != 502 || refusal.Code != "UPSTREAM_REFUSED" || refusal.Action != api.ActionRetry || refusal.RetryAfter != 30 {
			t.Fatalf("HTTP %d was refused as %+v", status, refusal)
		}
	}
}

func TestAFaviconThatIsNoImageIsRefused(t *testing.T) {
	for name, serve := range map[string]http.HandlerFunc{
		"empty": func(http.ResponseWriter, *http.Request) {},
		"text": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			io.WriteString(w, "<html></html>")
		},
		"too large": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "image/png")
			w.Write(make([]byte, maxFaviconBytes+1))
		},
		// A redirect is not followed, even to an image.
		"redirect": func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/elsewhere.png" {
				w.Header().Set("Content-Type", "image/png")
				w.Write([]byte("\x89PNG\r\n\x1a\n"))
				return
			}
			http.Redirect(w, r, "/elsewhere.png", http.StatusFound)
		},
	} {
		reader := testReader(t, unused(t), serve)
		if _, err := reader.Favicon(context.Background(), "https://example.com"); refusalOf(err) == nil || refusalOf(err).Code != "UPSTREAM_REFUSED" {
			t.Fatalf("a %s favicon was read: %v", name, err)
		}
	}
}

func TestAFaviconDuckDuckGoCannotBeReachedIsUnavailable(t *testing.T) {
	reader := newReader(testKey, "http://127.0.0.1:1")
	_, err := reader.Favicon(context.Background(), "https://example.com")
	if refusal := refusalOf(err); refusal == nil || refusal.Action != api.ActionRetry {
		t.Fatalf("an unreachable DuckDuckGo refused %v", err)
	}
}

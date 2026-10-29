package pages

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	sdk "github.com/context-dot-dev/context-go-sdk"
	"github.com/context-dot-dev/context-go-sdk/option"
)

const (
	apiResponseSizeMultiplier = 2
	maxAPIResponseOverhead    = 64 * 1024
	maxSupportedBodyBytes     = (math.MaxInt64 - maxAPIResponseOverhead - 1) / apiResponseSizeMultiplier
)

// contextDev reads pages through the context.dev web scraping API
// (https://docs.context.dev/api-reference/web-scraping/html), which extracts
// a page's metadata, so the harness never contacts the page's host and
// never parses its HTML. Every context.dev call goes through it.
type contextDev struct {
	sdk     sdk.Client
	timeout time.Duration
}

// scraped is a page's final URL and the metadata context.dev extracted.
type scraped struct {
	URL         string
	ContentType string
	Title       string
	Description string
	SiteName    string
	Image       string
}

func newContextDev(apiKey string, timeout time.Duration, opts ...option.RequestOption) *contextDev {
	return &contextDev{
		sdk:     sdk.NewClient(append([]option.RequestOption{option.WithAPIKey(apiKey)}, opts...)...),
		timeout: timeout,
	}
}

// scrape reads targetURL with zero data retention enforced: zdr=enabled
// keeps the request out of shared caches and retained usage logs, and
// maxAgeMs=0 asks for a fresh scrape. Requests fail with ZDR_NOT_ENABLED
// until zero data retention is enabled for the organization.
func (c *contextDev) scrape(ctx context.Context, targetURL string, maxBodyBytes int64) (*scraped, error) {
	if maxBodyBytes <= 0 || maxBodyBytes > maxSupportedBodyBytes {
		return nil, errors.New("invalid maximum body size")
	}
	maxAPIResponseBytes := maxBodyBytes*apiResponseSizeMultiplier + maxAPIResponseOverhead
	page, err := c.sdk.Web.WebScrapeHTML(ctx, sdk.WebWebScrapeHTMLParams{
		URL:      targetURL,
		MaxAgeMs: sdk.Int(0),
	},
		// The pinned SDK has no typed zdr parameter.
		option.WithQuery("zdr", "enabled"),
		option.WithHTTPClient(&limitedHTTPClient{
			inner:    &http.Client{Timeout: c.timeout},
			maxBytes: maxAPIResponseBytes,
		}),
	)
	if err != nil {
		var apiErr *sdk.Error
		if errors.As(err, &apiErr) {
			// apiErr's message holds the request's URL, which carries the
			// page's, and the raw response; neither may reach a log.
			return nil, fmt.Errorf("context.dev request failed with status %d", apiErr.StatusCode)
		}
		// A transport error's message holds the request's URL too.
		return nil, errors.New("context.dev request failed")
	}
	finalURL := page.Metadata.FinalURL
	if finalURL == "" {
		finalURL = page.URL
	}
	return &scraped{
		URL:         finalURL,
		ContentType: contentTypeFor(page.Type),
		Title:       page.Metadata.Title,
		Description: page.Metadata.Description,
		SiteName:    page.Metadata.SiteName,
		Image:       page.Metadata.Image,
	}, nil
}

// contentTypeFor maps the content type context.dev detected to a MIME type.
func contentTypeFor(detected sdk.WebWebScrapeHTMLResponseType) string {
	switch detected {
	case sdk.WebWebScrapeHTMLResponseTypeHTML:
		return "text/html"
	case sdk.WebWebScrapeHTMLResponseTypeXml:
		return "application/xml"
	case sdk.WebWebScrapeHTMLResponseTypeJson:
		return "application/json"
	case sdk.WebWebScrapeHTMLResponseTypeText:
		return "text/plain"
	default:
		return "application/octet-stream"
	}
}

// limitedHTTPClient bounds how many response bytes the SDK reads, so a
// misbehaving upstream cannot exhaust the enclave's memory.
type limitedHTTPClient struct {
	inner    *http.Client
	maxBytes int64
}

func (c *limitedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.inner.Do(req)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}
	// One extra byte lets a response of exactly maxBytes reach EOF while
	// anything larger fails on the next read.
	resp.Body = &limitedReadCloser{inner: resp.Body, remaining: c.maxBytes + 1}
	return resp, nil
}

type limitedReadCloser struct {
	inner     io.ReadCloser
	remaining int64
}

func (l *limitedReadCloser) Read(p []byte) (int, error) {
	if l.remaining <= 0 {
		return 0, errors.New("context.dev response too large")
	}
	if int64(len(p)) > l.remaining {
		p = p[:l.remaining]
	}
	n, err := l.inner.Read(p)
	l.remaining -= int64(n)
	if l.remaining <= 0 {
		// The extra byte was read, so the response exceeds maxBytes even
		// when this read also reached EOF.
		return n, errors.New("context.dev response too large")
	}
	return n, err
}

func (l *limitedReadCloser) Close() error { return l.inner.Close() }

// Package pages reads what a link preview shows of a web page, through
// context.dev, and a site's favicon, through DuckDuckGo, so the harness never
// contacts a page's host. context.dev receives the page's URL, with zero data
// retention and no shared cache; DuckDuckGo receives only its host. Nothing is
// cached, and no URL is logged.
package pages

import (
	"context"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/context-dot-dev/context-go-sdk/option"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

const (
	fetchTimeout               = 15 * time.Second
	maxPageBytes         int64 = 5 << 20
	maxFaviconBytes      int64 = 256 << 10
	duckDuckGoIcons            = "https://icons.duckduckgo.com"
	maxServerErrorStatus       = 599
)

// Status of a favicon the reader found, or found the site has none of.
const (
	FaviconFound   = "found"
	FaviconMissing = "missing"
)

// Reader reads pages and favicons for the harness's callers.
type Reader struct {
	contextDev *contextDev
	icons      *http.Client
	iconsURL   string
}

// New reads pages with the harness's context.dev key.
func New(apiKey string) *Reader {
	return newReader(apiKey, duckDuckGoIcons)
}

func newReader(apiKey, iconsURL string, contextDevOptions ...option.RequestOption) *Reader {
	return &Reader{
		contextDev: newContextDev(apiKey, fetchTimeout, contextDevOptions...),
		icons: &http.Client{
			Timeout: fetchTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		iconsURL: iconsURL,
	}
}

// Details is what a link preview shows of a page. A detail the page lacks
// is nil.
type Details struct {
	// URL is where the page ended up, after its redirects.
	URL         string
	Title       *string
	Description *string
	SiteName    *string
	Image       *string
}

// Favicon is a site's favicon, as the harness's protocol sends it: Bytes
// and ContentType are empty when the site has none.
type Favicon struct {
	Status      string `json:"status"`
	Bytes       []byte `json:"favicon_bytes"`
	ContentType string `json:"favicon_content_type"`
}

func refused(err error) error {
	return api.Err(400, "BAD_REQUEST", "The page's URL is not allowed: "+err.Error())
}

// Details reads a page's details.
func (r *Reader) Details(ctx context.Context, rawURL string) (*Details, error) {
	target, err := NormalizeURL(rawURL)
	if err != nil {
		return nil, refused(err)
	}
	unreadable := api.Err(502, "UPSTREAM_REFUSED", "The page's details could not be read")
	page, err := r.contextDev.scrape(ctx, target, maxPageBytes)
	if err != nil {
		return nil, unreadable
	}
	contentType, _, err := mime.ParseMediaType(page.ContentType)
	if err != nil || !strings.EqualFold(contentType, "text/html") {
		return nil, unreadable
	}
	pageURL := page.URL
	if pageURL == "" {
		pageURL = target
	}
	if pageURL, err = NormalizeURL(pageURL); err != nil {
		return nil, unreadable
	}
	base, err := url.Parse(pageURL)
	if err != nil {
		return nil, unreadable
	}
	details := &Details{URL: pageURL, Title: present(page.Title), Description: present(page.Description), SiteName: present(page.SiteName)}
	if image := strings.TrimSpace(page.Image); image != "" {
		if ref, err := url.Parse(image); err == nil {
			if resolved, err := NormalizeURL(base.ResolveReference(ref).String()); err == nil {
				details.Image = &resolved
			}
		}
	}
	return details, nil
}

func present(value string) *string {
	if value = strings.TrimSpace(value); value == "" {
		return nil
	}
	return &value
}

// Favicon reads the favicon of a page's site. A favicon DuckDuckGo cannot
// serve for now is refused with when to ask again.
func (r *Reader) Favicon(ctx context.Context, rawURL string) (*Favicon, error) {
	target, err := NormalizeURL(rawURL)
	if err != nil {
		return nil, refused(err)
	}
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, refused(err)
	}
	unavailable := api.Err(502, "UPSTREAM_REFUSED", "The site's favicon is unavailable")
	malformed := api.Err(502, "UPSTREAM_REFUSED", "The site's favicon is not an image")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.iconsURL+"/ip3/"+url.PathEscape(parsed.Hostname())+".ico", nil)
	if err != nil {
		return nil, malformed
	}
	resp, err := r.icons.Do(req)
	if err != nil {
		return nil, unavailable
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone:
		return &Favicon{Status: FaviconMissing, Bytes: []byte{}}, nil
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode >= http.StatusInternalServerError && resp.StatusCode <= maxServerErrorStatus:
		return nil, api.WithRetryAfter(unavailable, resp.Header)
	case resp.StatusCode != http.StatusOK:
		return nil, malformed
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFaviconBytes+1))
	if err != nil {
		return nil, unavailable
	}
	if len(body) == 0 || int64(len(body)) > maxFaviconBytes {
		return nil, malformed
	}
	contentType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(contentType, "image/") {
		if contentType = http.DetectContentType(body); !strings.HasPrefix(contentType, "image/") {
			return nil, malformed
		}
	}
	return &Favicon{Status: FaviconFound, Bytes: body, ContentType: contentType}, nil
}

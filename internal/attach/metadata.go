package attach

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/pages"
)

// Pages reads what a link preview shows of a page, and a site's favicon.
type Pages interface {
	Details(ctx context.Context, url string) (*pages.Details, error)
	Favicon(ctx context.Context, url string) (*pages.Favicon, error)
}

// Favicon reads a site's favicon for a caller with chat access.
func (at *Service) Favicon(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *PageRequest) (any, error) {
	if _, err := at.Auth.Inference(ctx, p); err != nil {
		return nil, err
	}
	return at.Pages.Favicon(ctx, in.URL)
}

// LinkPreview reads a page's details for a link preview that holds none, as
// the previous web client's did, for a caller with chat access.
func (at *Service) LinkPreview(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *PageRequest) (any, error) {
	if _, err := at.Auth.Inference(ctx, p); err != nil {
		return nil, err
	}
	return at.LinkDetails(ctx, in.URL)
}

// LinkDetails are a page's details, as a link preview shows them.
func (at *Service) LinkDetails(ctx context.Context, url string) (any, error) {
	details, err := at.Pages.Details(ctx, url)
	if err != nil {
		return nil, err
	}
	return api.Object{"url": url, "title": details.Title, "description": details.Description, "siteName": details.SiteName, "image": details.Image}, nil
}

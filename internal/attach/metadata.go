package attach

import (
	"context"
	"encoding/json"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

func (at *Service) Favicon(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *PageRequest) (any, error) {
	credential, err := at.Auth.Inference(ctx, p)
	if err != nil {
		return nil, err
	}
	ctx = auth.WithCallerKey(ctx, string(credential.Key))
	var result json.RawMessage
	err = at.Models.ServiceCall(ctx, "metadata", "/favicon", PageRequest{URL: in.URL}, &result)
	return result, err
}

// LinkPreview reads a page's details for a link preview that holds none, as
// the previous web client's did.
func (at *Service) LinkPreview(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *PageRequest) (any, error) {
	credential, err := at.Auth.Inference(ctx, p)
	if err != nil {
		return nil, err
	}
	return at.LinkDetails(auth.WithCallerKey(ctx, string(credential.Key)), in.URL)
}

// LinkDetails are a page's details, read by the metadata service, as a link
// preview shows them.
func (at *Service) LinkDetails(ctx context.Context, url string) (any, error) {
	var response api.Object
	if err := at.Models.ServiceCall(ctx, "metadata", "/metadata", api.Object{"url": url}, &response); err != nil {
		return nil, err
	}
	result := api.Obj(response["result"])
	if len(result) == 0 {
		result = response
	}
	return api.Object{"url": url, "title": result["title"], "description": result["description"], "siteName": api.FirstString(result["siteName"], result["site_name"]), "image": result["image"]}, nil
}

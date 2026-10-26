package main

import (
	"context"
	"encoding/json"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

func (h *harness) favicon(ctx context.Context, p *principal, _ contentKey, in *pageRequest) (any, error) {
	credential, err := h.auth.inference(ctx, p)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, apiKeyKey{}, string(credential.Key))
	var result json.RawMessage
	err = h.serviceCall(ctx, "metadata", "/favicon", pageRequest{URL: in.URL}, &result)
	return result, err
}

// linkPreview reads a page's details for a link preview that holds none, as
// the previous web client's did.
func (h *harness) linkPreview(ctx context.Context, p *principal, _ contentKey, in *pageRequest) (any, error) {
	credential, err := h.auth.inference(ctx, p)
	if err != nil {
		return nil, err
	}
	return h.linkDetails(context.WithValue(ctx, apiKeyKey{}, string(credential.Key)), in.URL)
}

// linkDetails are a page's details, read by the metadata service, as a link
// preview shows them.
func (h *harness) linkDetails(ctx context.Context, url string) (any, error) {
	var response api.Object
	if err := h.serviceCall(ctx, "metadata", "/metadata", api.Object{"url": url}, &response); err != nil {
		return nil, err
	}
	result := api.Obj(response["result"])
	if len(result) == 0 {
		result = response
	}
	return api.Object{"url": url, "title": result["title"], "description": result["description"], "siteName": api.FirstString(result["siteName"], result["site_name"]), "image": result["image"]}, nil
}

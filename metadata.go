package main

import (
	"context"
	"encoding/json"
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
	var response object
	if err := h.serviceCall(ctx, "metadata", "/metadata", object{"url": url}, &response); err != nil {
		return nil, err
	}
	result := obj(response["result"])
	if len(result) == 0 {
		result = response
	}
	return object{"url": url, "title": result["title"], "description": result["description"], "siteName": firstString(result["siteName"], result["site_name"]), "image": result["image"]}, nil
}

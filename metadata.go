package main

import (
	"context"
	"encoding/json"
	"strings"
)

func (h *harness) favicon(ctx context.Context, p *principal, _ contentKey, in *faviconRequest) (any, error) {
	if strings.TrimSpace(in.URL) == "" {
		return nil, apiErr(400, "BAD_REQUEST", "A page URL is required")
	}
	credential, err := h.auth.inference(ctx, p)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, apiKeyKey{}, string(credential.Key))
	var result json.RawMessage
	err = h.serviceCall(ctx, "metadata", "/favicon", faviconRequest{URL: in.URL}, &result)
	return result, err
}

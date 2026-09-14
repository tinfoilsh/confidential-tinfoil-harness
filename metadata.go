package main

import (
	"context"
	"strings"
)

func (h *harness) favicon(ctx context.Context, p *principal, _ contentKey, in object) (any, error) {
	if err := validateFields(in, "url", "", ""); err != nil {
		return nil, err
	}
	if strings.TrimSpace(str(in["url"])) == "" {
		return nil, apiErr(400, "BAD_REQUEST", "A page URL is required")
	}
	credential, err := h.auth.inference(ctx, p)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, apiKeyKey{}, string(credential.Key))
	var result object
	err = h.serviceCall(ctx, "metadata", "/favicon", object{"url": in["url"]}, &result)
	return result, err
}

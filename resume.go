package main

import "context"

func (h *harness) reserveThreadID(ctx context.Context, p *principal, key contentKey, nonce, digest string) (string, error) {
	id := rowID()
	_, err := h.mutate(ctx, p, key, "profile", "profile", true, nil, func(data object) error {
		index := obj(data["harnessThreadRequests"])
		if existing := obj(index[nonce]); len(existing) > 0 {
			if str(existing["digest"]) != digest {
				return apiErr(409, "REVISION_CONFLICT", "The request nonce was already used")
			}
			id = str(existing["threadId"])
			return nil
		}
		if len(index) >= 10000 {
			return apiErr(409, "REVISION_CONFLICT", "The thread request index is full")
		}
		index[nonce] = object{"threadId": id, "digest": digest}
		data["harnessThreadRequests"] = index
		return nil
	})
	return id, err
}

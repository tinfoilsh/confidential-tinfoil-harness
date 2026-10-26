package main

import (
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

func (h *harness) handleTurn(w http.ResponseWriter, r *http.Request) {
	p, err := h.auth.Authenticate(r, true)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	credential, err := h.auth.Inference(r.Context(), p)
	if err != nil {
		api.Respond(w, nil, h.withQuota(p, credential.Limit, err))
		return
	}
	body, err := api.ReadBody(w, r, domain.MaxTurnBody)
	defer clear(body)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	in := &turnRequest{}
	defer in.ClearKey()
	if err := api.DecodeBody(body, in); err != nil {
		api.Respond(w, nil, err)
		return
	}
	if in.Options.Locale == "" {
		in.Options.Locale = r.Header.Get("Accept-Language")
	}
	if err := in.validate(); err != nil {
		api.Respond(w, nil, err)
		return
	}
	if p.Anonymous && !in.Local {
		api.Respond(w, nil, errAnonymousCloud)
		return
	}
	var key auth.ContentKey
	if !p.Anonymous && !in.Local || len(in.Key) != 0 {
		key, err = auth.DecodeContentKey(in.Key)
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
	}
	defer clear(key[:])
	in.ClearKey()
	if err := in.validateContent(); err != nil {
		api.Respond(w, nil, err)
		return
	}
	accepted, err := h.acceptTurn(r.Context(), p, credential, key, in)
	clear(key[:])
	if err != nil {
		api.Respond(w, nil, h.withQuota(p, api.RateLimit{}, err))
		return
	}
	defer accepted.log.log.detach()
	follow(w, r, accepted.log, 0)
}

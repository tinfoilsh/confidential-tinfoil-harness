package engine

import (
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

func (eng *Engine) HandleTurn(w http.ResponseWriter, r *http.Request) {
	p, err := eng.Auth.Authenticate(r, true)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	credential, err := eng.Auth.Inference(r.Context(), p)
	if err != nil {
		api.Respond(w, nil, auth.WithQuota(p, credential.Limit, err))
		return
	}
	body, err := api.ReadBody(w, r, domain.MaxTurnBody)
	defer clear(body)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	in := &TurnRequest{}
	defer in.ClearKey()
	if err := api.DecodeBody(body, in); err != nil {
		api.Respond(w, nil, err)
		return
	}
	if in.Options.Locale == "" {
		in.Options.Locale = r.Header.Get("Accept-Language")
	}
	if err := in.Validate(); err != nil {
		api.Respond(w, nil, err)
		return
	}
	if p.Anonymous && !in.Local {
		api.Respond(w, nil, auth.ErrAnonymousCloud)
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
	accepted, err := eng.AcceptTurn(r.Context(), p, credential, key, in)
	clear(key[:])
	if err != nil {
		api.Respond(w, nil, auth.WithQuota(p, api.RateLimit{}, err))
		return
	}
	defer accepted.Log.Frames.Detach()
	Follow(w, r, accepted.Log, 0)
}

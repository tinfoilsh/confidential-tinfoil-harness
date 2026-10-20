package main

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"slices"
	"strings"

	"github.com/google/uuid"
)

func (h *harness) handleTurn(w http.ResponseWriter, r *http.Request) {
	p, err := h.auth.authenticate(r, true)
	if err != nil {
		respond(w, nil, err)
		return
	}
	in, err := decodeJSONWithin(w, r, maxTurnBody)
	if err != nil {
		respond(w, nil, err)
		return
	}
	if _, err := uuid.Parse(str(in["clientRequestId"])); err != nil {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "clientRequestId must be a UUID"))
		return
	}
	kind := firstString(in["kind"], "send")
	in["kind"] = kind
	if !slices.Contains([]string{"send", "edit", "regenerate", "continue", "resolve", "retryToolCall", "ask"}, kind) {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "Unknown turn kind"))
		return
	}
	local := boolean(in["local"], false)
	// An ask is never stored, so it may still say so.
	if boolean(in["ephemeral"], false) && kind != "ask" {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "Temporary chats are kept on the device"))
		return
	}
	if p.Anonymous && !local {
		respond(w, nil, apiErr(401, "UNAUTHENTICATED", "Anonymous chats are kept on the device"))
		return
	}
	var key contentKey
	if !p.Anonymous && !local || str(in["key"]) != "" {
		key, err = decodeContentKey(str(in["key"]))
		if err != nil {
			respond(w, nil, err)
			return
		}
	}
	defer clear(key[:])
	if _, err := requiredID(in, "threadId"); err != nil {
		respond(w, nil, err)
		return
	}
	if err := validateTurn(in); err != nil {
		respond(w, nil, err)
		return
	}
	accepted, err := h.acceptTurn(r.Context(), p, key, in)
	if err != nil {
		respond(w, nil, err)
		return
	}
	follow(w, r, accepted.log)
}

func validateTurn(in object) error {
	if err := validateFields(in, "kind clientRequestId content quote messageId toolCallId", "ephemeral local", "threadId projectId presetId"); err != nil {
		return err
	}
	local := boolean(in["local"], false)
	if local && in["projectId"] != nil {
		return apiErr(400, "BAD_REQUEST", "Projects require cloud storage")
	}
	for _, field := range []string{"thread", "profile"} {
		value, ok := in[field]
		if !ok {
			continue
		}
		if !local {
			return apiErr(400, "BAD_REQUEST", field+" is only accepted for local threads")
		}
		if !validObject(value) {
			return apiErr(400, "BAD_REQUEST", field+" must be an object")
		}
	}
	if err := validateLocalThread(obj(in["thread"])); err != nil {
		return err
	}
	if err := validateProfile(profileView(obj(in["profile"]))); err != nil {
		return err
	}
	if value, ok := in["options"]; ok {
		if !validObject(value) {
			return apiErr(400, "BAD_REQUEST", "options must be an object")
		}
		if err := validateFields(obj(value), "model autoIntelligence reasoningEffort timezone", "thinking webSearch codeExecution piiCheck genUI", ""); err != nil {
			return err
		}
	}
	if in["kind"] == "resolve" {
		if !validObject(in["resolution"]) {
			return apiErr(400, "BAD_REQUEST", "resolution must be an object")
		}
		if err := validateFields(obj(in["resolution"]), "text", "", ""); err != nil {
			return err
		}
	}

	for _, field := range []string{"content", "quote"} {
		if v, ok := in[field]; ok {
			if _, ok := v.(string); !ok {
				return apiErr(400, "BAD_REQUEST", field+" must be a string")
			}
		}
	}
	if v, ok := in["attachments"]; ok {
		if _, ok := v.([]any); !ok {
			return apiErr(400, "BAD_REQUEST", "attachments must be an array")
		}
		for _, item := range arr(v) {
			if err := validateSentAttachment(item); err != nil {
				return err
			}
		}
	}
	if in["kind"] == "send" && strings.TrimSpace(str(in["content"])) == "" && len(arr(in["attachments"])) == 0 {
		return apiErr(400, "BAD_REQUEST", "A message or attachment is required")
	}
	return nil
}

func requestDigest(in object) string {
	copy := clone(in)
	delete(copy, "key")
	digest := sha256.Sum256(raw(copy))
	return hex.EncodeToString(digest[:])
}

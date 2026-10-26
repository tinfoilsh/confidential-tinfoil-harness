package main

import (
	"encoding/json"
	"errors"
	"fmt"
)

type apiError struct {
	HTTP    int    `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
	// Action is what a client does about the error when it has no handling
	// of its own for the code.
	Action     string `json:"action"`
	Kind       string `json:"kind,omitempty"`
	ResetsAt   string `json:"resetsAt,omitempty"`
	RetryAfter int    `json:"retryAfter,omitempty"`
	Status     int    `json:"status,omitempty"`
	KeyID      string `json:"keyId,omitempty"`
	Upstream   string `json:"upstreamCode,omitempty"`
	// RateLimit is the caller's quota as an exhausted one's refusal leaves it.
	RateLimit *rateLimit `json:"rateLimit,omitempty"`

	missing []string // attachment ids sync did not find under the pushed chat
}

// The actions protocol/openapi.yaml describes as ErrorAction.
const (
	actionReauthenticate = "reauthenticate"
	actionReload         = "reload"
	actionRetry          = "retry"
	actionWait           = "wait"
	actionUnlock         = "unlock"
	actionRevise         = "revise"
	actionStop           = "stop"
)

// errorActions is what a client does about each code, unless an error says
// otherwise.
var errorActions = map[string]string{
	"ALREADY_APPLIED":         actionReload,
	"ARCHIVE_TOO_LARGE":       actionRevise,
	"ATTACHMENT_TOO_LARGE":    actionRevise,
	"ATTACHMENT_UNSUPPORTED":  actionRevise,
	"ATTACHMENTS_UNAVAILABLE": actionRetry,
	"BAD_REQUEST":             actionStop,
	"CHAT_FLAGGED":            actionStop,
	"CONTEXT_TOO_LARGE":       actionRevise,
	"CREDENTIAL_REQUIRED":     actionReauthenticate,
	"IMPORT_INVALID":          actionRevise,
	"IMPORT_TOO_LARGE":        actionRevise,
	"KEY_MISMATCH":            actionUnlock,
	"KEY_REQUIRED":            actionUnlock,
	"MAX_TURNS":               actionRetry,
	"MISSING_ATTACHMENT":      actionStop,
	"MODEL_UNAVAILABLE":       actionRevise,
	"PAYLOAD_TOO_LARGE":       actionRevise,
	"PROJECTS_REQUIRE_CLOUD":  actionStop,
	"QUEUE_FULL":              actionRetry,
	"QUOTA_EXHAUSTED":         actionWait,
	"RECORD_UNSUPPORTED":      actionStop,
	"RATE_LIMITED":            actionRetry,
	"REQUEST_TIMEOUT":         actionRetry,
	"REVISION_CONFLICT":       actionReload,
	"RUN_IN_FLIGHT":           actionReload,
	"RUN_LOG_FULL":            actionStop,
	"RUN_NOT_FOUND":           actionReload,
	"SEARCH_INDEX_FAILED":     actionRetry,
	"SYNC_OUTCOME_UNKNOWN":    actionRetry,
	"TEMP_SPACE_BUSY":         actionRetry,
	"THREAD_EXISTS":           actionStop,
	"THREAD_NOT_FOUND":        actionStop,
	"THREAD_TOO_LARGE":        actionRevise,
	"TOOL_UNAVAILABLE":        actionRetry,
	"UNAUTHENTICATED":         actionReauthenticate,
	"UPSTREAM_REFUSED":        actionRetry,
}

func (e *apiError) Error() string { return e.Message }
func apiErr(status int, code, message string) *apiError {
	return &apiError{HTTP: status, Code: code, Message: message, Action: errorActions[code]}
}

// withAction has a client take another action about a new error than its
// code's.
func (e *apiError) withAction(action string) *apiError {
	e.Action = action
	return e
}
func asAPIError(err error) *apiError {
	var a *apiError
	if errors.As(err, &a) {
		return a
	}
	var refusal *refusal
	if errors.As(err, &refusal) {
		e := downstreamError(refusal.status, []byte(refusal.detail))
		if refusal.retryAfter > 0 {
			e.RetryAfter = refusal.retryAfter
		}
		return e
	}
	return apiErr(502, "UPSTREAM_REFUSED", "A downstream request failed")
}

func downstreamError(status int, body []byte) *apiError {
	var in object
	json.Unmarshal(body, &in)
	if nested := obj(in["error"]); len(nested) > 0 {
		in = nested
	}
	code := str(in["code"])
	e := apiErr(502, "UPSTREAM_REFUSED", "A downstream service refused the request")
	e.Status = status
	switch code {
	case "STALE_KEY", "EXISTING_DATA_UNDER_OTHER_KEY", "UNKNOWN_KEY", "KEY_MISMATCH":
		e = apiErr(409, "KEY_MISMATCH", "The stored data uses another content key")
		e.KeyID = firstString(in["current_key_id"], in["key_id"])
	case "STALE_BLOB", "SYNC_CONFLICT":
		e = apiErr(409, "REVISION_CONFLICT", "The stored row changed")
	case "MISSING_ATTACHMENT":
		e = apiErr(409, "MISSING_ATTACHMENT", "The chat references attachments that are not stored with it")
		for _, id := range arr(in["missing_attachments"]) {
			e.missing = append(e.missing, str(id))
		}
	case "PAYLOAD_TOO_LARGE":
		e = apiErr(413, "THREAD_TOO_LARGE", "The chat is too large to store")
	case "SYNC_PROTOCOL_UPGRADE_REQUIRED":
		e.Message, e.Action = "The sync service requires a newer protocol", actionStop
	case "UPSTREAM":
		if str(in["reason"]) == "outcome_unknown" {
			// The write may have been stored. Retrying reapplies the change
			// to a freshly pulled row, so retrying is safe.
			e = apiErr(503, "SYNC_OUTCOME_UNKNOWN", "The sync service may not have stored the change")
		}
	default:
		if refused := openAIRefusal(status, body); refused != nil {
			if refused.RetryAfter == 0 {
				refused.RetryAfter = integer(in["retryAfter"])
			}
			return refused
		}
		switch status {
		case 401:
			e = apiErr(401, "UNAUTHENTICATED", "Authentication is required")
		case 404:
			e = apiErr(404, "THREAD_NOT_FOUND", "The requested data was not found")
		case 412:
			e = apiErr(409, "REVISION_CONFLICT", "The stored row changed")
		case 429:
			e = apiErr(429, "QUOTA_EXHAUSTED", "The request quota is exhausted")
			e.Kind = firstString(in["kind"], in["type"])
			e.ResetsAt = firstString(in["resetsAt"], in["resets_at"])
			e.RetryAfter = integer(in["retryAfter"])
		}
		refusedAsSent(e, status)
	}
	return e
}

type refusal struct {
	status     int
	detail     string
	retryAfter int
}

func (r *refusal) Error() string { return fmt.Sprintf("gateway returned %d: %s", r.status, r.detail) }

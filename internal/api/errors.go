package api

import (
	"encoding/json"
	"errors"
	"fmt"
)

type Error struct {
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
	RateLimit *RateLimit `json:"rateLimit,omitempty"`

	Missing []string `json:"-"` // attachment ids sync did not find under the pushed chat
}

// The actions protocol/openapi.yaml describes as ErrorAction.
const (
	ActionReauthenticate = "reauthenticate"
	ActionReload         = "reload"
	ActionRetry          = "retry"
	ActionWait           = "wait"
	ActionUnlock         = "unlock"
	ActionRevise         = "revise"
	ActionStop           = "stop"
)

// Actions is what a client does about each code, unless an error says
// otherwise.
var Actions = map[string]string{
	"ALREADY_APPLIED":         ActionReload,
	"ARCHIVE_TOO_LARGE":       ActionRevise,
	"ATTACHMENT_TOO_LARGE":    ActionRevise,
	"ATTACHMENT_UNSUPPORTED":  ActionRevise,
	"ATTACHMENTS_UNAVAILABLE": ActionRetry,
	"BAD_REQUEST":             ActionStop,
	"CHAT_FLAGGED":            ActionStop,
	"CONTEXT_TOO_LARGE":       ActionRevise,
	"CREDENTIAL_REQUIRED":     ActionReauthenticate,
	"IMPORT_INVALID":          ActionRevise,
	"IMPORT_TOO_LARGE":        ActionRevise,
	"KEY_MISMATCH":            ActionUnlock,
	"KEY_REQUIRED":            ActionUnlock,
	"MAX_TURNS":               ActionRetry,
	"MISSING_ATTACHMENT":      ActionStop,
	"MODEL_UNAVAILABLE":       ActionRevise,
	"PAYLOAD_TOO_LARGE":       ActionRevise,
	"PROJECTS_REQUIRE_CLOUD":  ActionStop,
	"QUEUE_FULL":              ActionRetry,
	"QUOTA_EXHAUSTED":         ActionWait,
	"RECORD_UNSUPPORTED":      ActionStop,
	"RATE_LIMITED":            ActionRetry,
	"REQUEST_TIMEOUT":         ActionRetry,
	"REVISION_CONFLICT":       ActionReload,
	"RUN_IN_FLIGHT":           ActionReload,
	"RUN_LOG_FULL":            ActionStop,
	"RUN_NOT_FOUND":           ActionReload,
	"SEARCH_INDEX_FAILED":     ActionRetry,
	"SYNC_OUTCOME_UNKNOWN":    ActionRetry,
	"TEMP_SPACE_BUSY":         ActionRetry,
	"THREAD_EXISTS":           ActionStop,
	"THREAD_NOT_FOUND":        ActionStop,
	"THREAD_TOO_LARGE":        ActionRevise,
	"TOOL_UNAVAILABLE":        ActionRetry,
	"UNAUTHENTICATED":         ActionReauthenticate,
	"UPSTREAM_REFUSED":        ActionRetry,
}

func (e *Error) Error() string { return e.Message }
func Err(status int, code, message string) *Error {
	return &Error{HTTP: status, Code: code, Message: message, Action: Actions[code]}
}

// withAction has a client take another action about a new error than its
// code's.
func (e *Error) WithAction(action string) *Error {
	e.Action = action
	return e
}
func AsError(err error) *Error {
	var a *Error
	if errors.As(err, &a) {
		return a
	}
	var refusal *Refusal
	if errors.As(err, &refusal) {
		e := DownstreamError(refusal.Status, []byte(refusal.Detail))
		if refusal.RetryAfter > 0 {
			e.RetryAfter = refusal.RetryAfter
		}
		return e
	}
	return Err(502, "UPSTREAM_REFUSED", "A downstream request failed")
}

func DownstreamError(status int, body []byte) *Error {
	var in Object
	json.Unmarshal(body, &in)
	if nested := Obj(in["error"]); len(nested) > 0 {
		in = nested
	}
	code := Str(in["code"])
	e := Err(502, "UPSTREAM_REFUSED", "A downstream service refused the request")
	e.Status = status
	switch code {
	case "STALE_KEY", "EXISTING_DATA_UNDER_OTHER_KEY", "UNKNOWN_KEY", "KEY_MISMATCH":
		e = Err(409, "KEY_MISMATCH", "The stored data uses another content key")
		e.KeyID = FirstString(in["current_key_id"], in["key_id"])
	case "STALE_BLOB", "SYNC_CONFLICT":
		e = Err(409, "REVISION_CONFLICT", "The stored row changed")
	case "MISSING_ATTACHMENT":
		e = Err(409, "MISSING_ATTACHMENT", "The chat references attachments that are not stored with it")
		for _, id := range Arr(in["missing_attachments"]) {
			e.Missing = append(e.Missing, Str(id))
		}
	case "PAYLOAD_TOO_LARGE":
		e = Err(413, "THREAD_TOO_LARGE", "The chat is too large to store")
	case "SYNC_PROTOCOL_UPGRADE_REQUIRED":
		e.Message, e.Action = "The sync service requires a newer protocol", ActionStop
	case "UPSTREAM":
		if Str(in["reason"]) == "outcome_unknown" {
			// The write may have been stored. Retrying reapplies the change
			// to a freshly pulled row, so retrying is safe.
			e = Err(503, "SYNC_OUTCOME_UNKNOWN", "The sync service may not have stored the change")
		}
	default:
		if refused := OpenAIRefusal(status, body); refused != nil {
			if refused.RetryAfter == 0 {
				refused.RetryAfter = Int(in["retryAfter"])
			}
			return refused
		}
		switch status {
		case 401:
			e = Err(401, "UNAUTHENTICATED", "Authentication is required")
		case 404:
			e = Err(404, "THREAD_NOT_FOUND", "The requested data was not found")
		case 412:
			e = Err(409, "REVISION_CONFLICT", "The stored row changed")
		case 429:
			e = Err(429, "QUOTA_EXHAUSTED", "The request quota is exhausted")
			e.Kind = FirstString(in["kind"], in["type"])
			e.ResetsAt = FirstString(in["resetsAt"], in["resets_at"])
			e.RetryAfter = Int(in["retryAfter"])
		}
		RefusedAsSent(e, status)
	}
	return e
}

type Refusal struct {
	Status     int
	Detail     string
	RetryAfter int
}

func (r *Refusal) Error() string { return fmt.Sprintf("gateway returned %d: %s", r.Status, r.Detail) }

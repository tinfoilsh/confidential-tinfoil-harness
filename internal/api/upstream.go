package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	maxRetryAfter      = 24 * 60 * 60 // seconds
	maxUpstreamMessage = 512
)

// RetryAfter reads one delay-seconds or HTTP-date Retry-After value in whole
// seconds, at least one so a retry never spins. Absent, repeated, or malformed
// values read as zero.
func RetryAfter(header http.Header) int {
	values := header.Values("Retry-After")
	if len(values) != 1 {
		return 0
	}
	value := strings.TrimSpace(values[0])
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		return int(max(1, min(seconds, maxRetryAfter)))
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(1, min(int(math.Ceil(time.Until(at).Seconds())), maxRetryAfter))
	}
	return 0
}

func WithRetryAfter(e *Error, header http.Header) *Error {
	if seconds := RetryAfter(header); seconds > 0 {
		e.RetryAfter = seconds
	}
	return e
}

// OpenAIRefusal maps the OpenAI error shape that the gateway, the router, and
// model and document enclaves answer with, nested or flat. Other bodies, such
// as sync's, keep their status mapping, as does a 401, which callers answer
// by refreshing the credential.
func OpenAIRefusal(status int, body []byte) *Error {
	var in Object
	if status == http.StatusUnauthorized || json.Unmarshal(body, &in) != nil {
		return nil
	}
	fields := Obj(in["error"])
	if len(fields) == 0 {
		if fields = in; Str(in["object"]) != "error" {
			return nil
		}
	}
	code, kind, message := Str(fields["code"]), Str(fields["type"]), Str(fields["message"])
	message = strings.ToValidUTF8(message[:min(len(message), maxUpstreamMessage)], "")
	e := Err(502, "UPSTREAM_REFUSED", "A downstream service refused the request")
	switch {
	case status == http.StatusPaymentRequired || code == "insufficient_quota" || kind == "insufficient_quota":
		e = Err(429, "QUOTA_EXHAUSTED", FirstString(message, "The request quota is exhausted"))
	case status == http.StatusTooManyRequests:
		e = Err(429, "RATE_LIMITED", FirstString(message, "Too many requests; retry shortly"))
	case status == http.StatusServiceUnavailable && code == "model_unavailable":
		e = Err(503, "MODEL_UNAVAILABLE", FirstString(message, "The model is temporarily unavailable")).WithAction(ActionRetry)
	case status == http.StatusServiceUnavailable && code == "server_is_overloaded":
		e = Err(503, "UPSTREAM_REFUSED", FirstString(message, "The model is overloaded"))
	case status/100 == 4 && (code == "context_length_exceeded" || strings.Contains(strings.ToLower(message), "context length")):
		e = Err(413, "CONTEXT_TOO_LARGE", FirstString(message, "The conversation exceeds the model's context window"))
	case (status/100 == 4 || strings.HasPrefix(code, "document_")) && message != "":
		// Document failures carry fixed messages that describe nothing in the file.
		e = Err(502, "UPSTREAM_REFUSED", message)
	}
	RefusedAsSent(e, status)
	e.Status = status
	if strings.HasPrefix(code, "document_") {
		e.Upstream = code
		// A document that failed to convert is the file's doing, as its
		// message says, so the user sends another.
		if e.Code == "UPSTREAM_REFUSED" {
			e.Action = ActionRevise
		}
	}
	return e
}

// RefusedAsSent has a client stop resending a request a downstream service
// refused with a 4xx it maps to nothing more specific, since sending it
// unchanged cannot succeed.
func RefusedAsSent(e *Error, status int) {
	if e.Code == "UPSTREAM_REFUSED" && status/100 == 4 {
		e.Action = ActionStop
	}
}

// StreamError maps an error event inside an accepted stream, whose type stands
// in for the status. It is never 401: refreshing the credential would replay a
// request the model already began answering.
func StreamError(data []byte) *Error {
	var in Object
	json.Unmarshal(data, &in)
	fields := Obj(in["error"])
	if len(fields) == 0 {
		fields = in
	}
	status := http.StatusBadGateway
	switch Str(fields["type"]) {
	case "invalid_request_error", "BadRequestError":
		status = http.StatusBadRequest
	case "insufficient_quota":
		status = http.StatusPaymentRequired
	case "rate_limit_error", "RateLimitError":
		status = http.StatusTooManyRequests
	case "service_unavailable_error", "ServiceUnavailableError":
		status = http.StatusServiceUnavailable
	}
	if code := Int(fields["code"]); code >= 400 && code < 600 && code != http.StatusUnauthorized {
		status = code
	}
	return DownstreamError(status, data)
}

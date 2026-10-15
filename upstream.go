package main

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

// retryAfter reads one delay-seconds or HTTP-date Retry-After value in whole
// seconds, at least one so a retry never spins. Absent, repeated, or malformed
// values read as zero.
func retryAfter(header http.Header) int {
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

func withRetryAfter(e *apiError, header http.Header) *apiError {
	if seconds := retryAfter(header); seconds > 0 {
		e.RetryAfter = seconds
	}
	return e
}

// openAIRefusal maps the OpenAI error shape that the gateway, the router, and
// model and document enclaves answer with, nested or flat. Other bodies, such
// as sync's, keep their status mapping, as does a 401, which callers answer
// by refreshing the credential.
func openAIRefusal(status int, body []byte) *apiError {
	var in object
	if status == http.StatusUnauthorized || json.Unmarshal(body, &in) != nil {
		return nil
	}
	fields := obj(in["error"])
	if len(fields) == 0 {
		if fields = in; str(in["object"]) != "error" {
			return nil
		}
	}
	code, kind, message := str(fields["code"]), str(fields["type"]), str(fields["message"])
	message = strings.ToValidUTF8(message[:min(len(message), maxUpstreamMessage)], "")
	e := apiErr(502, "UPSTREAM_REFUSED", "A downstream service refused the request")
	switch {
	case status == http.StatusPaymentRequired || code == "insufficient_quota" || kind == "insufficient_quota":
		e = apiErr(429, "QUOTA_EXHAUSTED", firstString(message, "The request quota is exhausted"))
	case status == http.StatusTooManyRequests:
		e = apiErr(429, "RATE_LIMITED", firstString(message, "Too many requests; retry shortly"))
	case status == http.StatusServiceUnavailable && code == "model_unavailable":
		e = apiErr(503, "MODEL_UNAVAILABLE", firstString(message, "The model is temporarily unavailable"))
	case status == http.StatusServiceUnavailable && code == "server_is_overloaded":
		e = apiErr(503, "UPSTREAM_REFUSED", firstString(message, "The model is overloaded"))
	case status/100 == 4 && (code == "context_length_exceeded" || strings.Contains(strings.ToLower(message), "context length")):
		e = apiErr(413, "CONTEXT_TOO_LARGE", firstString(message, "The conversation exceeds the model's context window"))
	case (status/100 == 4 || strings.HasPrefix(code, "document_")) && message != "":
		// Document failures carry fixed messages that describe nothing in the file.
		e = apiErr(502, "UPSTREAM_REFUSED", message)
	}
	e.Status = status
	if strings.HasPrefix(code, "document_") {
		e.Upstream = code
	}
	return e
}

// streamError maps an error event inside an accepted stream, whose type stands
// in for the status. It is never 401: refreshing the credential would replay a
// request the model already began answering.
func streamError(data []byte) *apiError {
	var in object
	json.Unmarshal(data, &in)
	fields := obj(in["error"])
	if len(fields) == 0 {
		fields = in
	}
	status := http.StatusBadGateway
	switch str(fields["type"]) {
	case "invalid_request_error", "BadRequestError":
		status = http.StatusBadRequest
	case "insufficient_quota":
		status = http.StatusPaymentRequired
	case "rate_limit_error", "RateLimitError":
		status = http.StatusTooManyRequests
	case "service_unavailable_error", "ServiceUnavailableError":
		status = http.StatusServiceUnavailable
	}
	if code := integer(fields["code"]); code >= 400 && code < 600 && code != http.StatusUnauthorized {
		status = code
	}
	return downstreamError(status, data)
}

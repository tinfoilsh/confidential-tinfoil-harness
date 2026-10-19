package main

import "slices"

// A local transcript carries its attachments inline, so a turn may be larger
// than other requests. Uploads already accept files of up to 50 MiB.
const maxTurnBody = 64 << 20

const maxLocalMessages = 10000

// Fields the harness owns are never taken from a client's copy of a thread.
var serverThreadFields = []string{"activeRun", "harnessRequests", "harnessRetry", "harnessHiddenCount", "projectId", "contextUsage"}

func validateLocalThread(thread object) error {
	if err := validateFields(thread, "title titleState model lastRequestId", "pinned webSearchEnabled", "presetId"); err != nil {
		return err
	}
	value, ok := thread["messages"]
	if !ok {
		return nil
	}
	messages, ok := value.([]any)
	if !ok {
		return apiErr(400, "BAD_REQUEST", "thread.messages must be an array")
	}
	if len(messages) > maxLocalMessages {
		return apiErr(413, "CONTEXT_TOO_LARGE", "The thread has too many messages")
	}
	for _, item := range messages {
		if !validObject(item) {
			return apiErr(400, "BAD_REQUEST", "Each message must be an object")
		}
		message := obj(item)
		if !slices.Contains([]string{"user", "assistant"}, str(message["role"])) {
			return apiErr(400, "BAD_REQUEST", "A message role must be user or assistant")
		}
		if err := validateFields(message, "id content", "", ""); err != nil {
			return err
		}
		if id := str(message["id"]); id != "" && !identifier(id) {
			return apiErr(400, "BAD_REQUEST", "A message id must be a valid identifier")
		}
		for _, field := range []string{"attachments", "timeline"} {
			if v, ok := message[field]; ok && v != nil {
				if _, ok := v.([]any); !ok {
					return apiErr(400, "BAD_REQUEST", field+" must be an array")
				}
			}
		}
	}
	return nil
}

// localThread is the row a local turn starts from. It keeps the client's
// fields, so the transcript returned to the client round-trips them.
func localThread(id string, thread object) *storedRow {
	data := clone(thread)
	for _, field := range serverThreadFields {
		delete(data, field)
	}
	if _, ok := data["messages"]; !ok {
		data["messages"] = []any{}
	}
	if str(data["createdAt"]) == "" && integer(data["createdAt"]) == 0 {
		data["createdAt"] = timestamp()
	}
	row := &storedRow{ID: id, Data: data}
	normalizeThread(row)
	return row
}

// localProfile keeps the settings a prompt reads. Display preferences and
// unknown fields from newer clients are ignored rather than refused.
func localProfile(profile object) object {
	out := object{}
	for _, field := range profileFields {
		if value, ok := profile[field]; ok {
			out[field] = value
		}
	}
	return out
}

// deviceThread is a local thread as its client stores it after a run. Image
// bytes are left out: the client holds them and puts them back by id.
func deviceThread(row *storedRow) object {
	thread := clone(normalizeThread(row))
	for _, message := range arr(thread["messages"]) {
		for _, a := range arr(obj(message)["attachments"]) {
			delete(obj(a), "base64")
			delete(obj(a), "inferenceMimeType")
		}
	}
	return thread
}

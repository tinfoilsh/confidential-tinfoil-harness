package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"
)

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

func messagesAt(log *run, through int) ([]any, bool, error) {
	frames, _, _ := log.log.read(0)
	if through >= len(frames) {
		return nil, false, apiErr(400, "BAD_REQUEST", "Last-Event-ID is beyond the run log")
	}
	base := []any{}
	hasOlder := false
	assembler := newMessageAssembler()
	for index, frame := range frames {
		if index > through {
			break
		}
		plain, err := log.open(index, frame)
		if err != nil {
			return nil, false, err
		}
		var e event
		if err := json.Unmarshal(plain, &e); err != nil {
			return nil, false, err
		}
		if e.Type == "MESSAGES_SNAPSHOT" {
			base = arr(e.Messages)
			if e.HasOlder != nil {
				hasOlder = *e.HasOlder
			}
			assembler = newMessageAssembler()
		} else {
			assembler.apply(e)
		}
	}
	for _, m := range assembler.messages() {
		id := m["id"]
		found := false
		for i, existing := range base {
			if obj(existing)["id"] == id {
				base[i] = mergeReplayedMessage(obj(existing), m)
				found = true
				break
			}
		}
		if !found {
			base = append(base, publicMessage(m))
		}
	}
	return base, hasOlder, nil
}

// Retry events replace one existing widget, keeping the rest of its message.
func mergeReplayedMessage(existing, patch object) object {
	result := clone(existing)
	blocks := arr(result["timeline"])
	for _, item := range arr(patch["timeline"]) {
		next := obj(item)
		found := false
		for i, item := range blocks {
			old := obj(item)
			if old["id"] == next["id"] || next["toolCallId"] != nil && old["toolCallId"] == next["toolCallId"] {
				for _, field := range []string{"arguments", "name", "complete"} {
					if value, ok := next[field]; ok {
						old[field] = value
					}
				}
				blocks[i] = old
				found = true
				break
			}
		}
		if !found {
			blocks = append(blocks, next)
		}
	}
	result["timeline"] = blocks
	return result
}

// A retry must not revive a deleted widget or overwrite a concurrent writer.
// The baseline lives in the encrypted row because the initial message window
// may not include the older message whose widget is being retried.
func replaceWidget(data object, messageID, callID, expected, replacement string) error {
	for _, message := range arr(data["messages"]) {
		m := obj(message)
		if m["id"] != messageID {
			continue
		}
		for _, item := range arr(m["timeline"]) {
			block := obj(item)
			if firstString(block["toolCallId"], block["id"]) != callID {
				continue
			}
			if block["arguments"] != expected && block["arguments"] != replacement {
				return apiErr(409, "REVISION_CONFLICT", "The widget changed during its retry")
			}
			block["arguments"] = replacement
			return nil
		}
	}
	return apiErr(409, "REVISION_CONFLICT", "The widget was removed during its retry")
}
func followChat(w http.ResponseWriter, r *http.Request, log *run, from int) {
	// A bootstrap event has no SSE id and describes exactly the cursor prefix.
	// Replayed deltas therefore append once even on a newly opened device.
	through := from - 1
	if through < 0 {
		frames, _, _ := log.log.read(0)
		for i, frame := range frames {
			plain, err := log.open(i, frame)
			if err != nil {
				respond(w, nil, err)
				return
			}
			var e event
			json.Unmarshal(plain, &e)
			if e.Type == "MESSAGES_SNAPSHOT" {
				through = i
				break
			}
		}
	}
	if through >= 0 {
		messages, hasOlder, err := messagesAt(log, through)
		if err != nil {
			respond(w, nil, err)
			return
		}
		if len(messages) > 50 {
			hasOlder = true
			messages = messages[len(messages)-50:]
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Accel-Buffering", "no")
		fmt.Fprintf(w, "data: %s\n\n", raw(event{Type: "MESSAGES_SNAPSHOT", Messages: messages, HasOlder: &hasOlder}))
		http.NewResponseController(w).Flush()
	}
	follow(w, r, log, from)
}

// A fresh session can finish the final CAS after a restart or after the Clerk
// token used to begin a long run expired. The output is recovered from the log.
func (h *harness) settleRecovered(ctx context.Context, p *principal, key contentKey, row *storedRow, log *run) error {
	if obj(row.Data["activeRun"])["runId"] != log.id {
		return nil
	}
	frames, _, closed := log.log.read(0)
	if closed == nil {
		return nil
	}
	if closed == errAbandoned {
		expiry, parseErr := time.Parse(time.RFC3339Nano, str(obj(row.Data["activeRun"])["expiresAt"]))
		if parseErr == nil && nowUTC().Before(expiry) {
			e := apiErr(409, "RUN_IN_FLIGHT", "The run may still be active on another replica")
			e.RetryAfter = 5
			return e
		}
	}
	assembler := newMessageAssembler()
	interrupted, failed := closed == errAbandoned, false
	for index, frame := range frames {
		plain, err := log.open(index, frame)
		if err != nil {
			return err
		}
		var e event
		if err := json.Unmarshal(plain, &e); err != nil {
			return err
		}
		if e.Type == "RUN_ERROR" {
			failed = true
		}
		if e.Type == "RUN_FINISHED" {
			interrupted = interrupted || boolean(obj(decodeValue(e.Metadata))["cancelled"], false)
		}
		assembler.apply(e)
	}
	_, err := h.mutate(ctx, p, key, "chat", row.ID, false, nil, func(data object) error {
		if obj(data["activeRun"])["runId"] != log.id {
			return nil
		}
		messages := arr(data["messages"])
		retry := obj(data["harnessRetry"])
		for _, m := range assembler.messages() {
			if retry["runId"] == log.id && retry["messageId"] == m["id"] {
				for _, item := range arr(m["timeline"]) {
					block := obj(item)
					if block["toolCallId"] == retry["toolCallId"] && boolean(block["complete"], false) {
						if err := replaceWidget(data, str(retry["messageId"]), str(retry["toolCallId"]), str(retry["arguments"]), str(block["arguments"])); err != nil {
							return err
						}
					}
				}
				continue
			}
			if index := slices.IndexFunc(messages, func(value any) bool { return obj(value)["id"] == m["id"] }); index >= 0 {
				// Normal runs never modify an existing assistant message. Without
				// a retry baseline there is no safe way to replace its arguments.
				continue
			}
			m["isInterrupted"], m["isError"] = interrupted, failed && !interrupted
			m["model"] = data["model"]
			messages = append(messages, m)
		}
		data["messages"], data["activeRun"] = messages, nil
		delete(data, "harnessRetry")
		return nil
	})
	return err
}

package main

import "slices"

// A local transcript carries its attachments inline, so a turn may be larger
// than other requests. Uploads already accept files of up to 50 MiB.
const maxTurnBody = 64 << 20

const maxLocalMessages = 10000

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

// deviceThread is a local thread as its client stores it after a run. Image
// bytes and document page images are left out, so a scanned document cannot
// fill the run's log: the client holds them and puts them back by id.
func deviceThread(t *Thread) Thread {
	thread := t.clone()
	thread.eachAttachment(func(a *Attachment) error {
		a.Base64, a.inferenceMimeType = "", ""
		for i := range a.Pages {
			a.Pages[i].Image = ""
		}
		return nil
	})
	return thread
}

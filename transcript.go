package main

// applyTurn makes the change a turn's kind makes to a transcript before its
// reply: a new or edited question, a resolved tool call, or the cut a
// regeneration or continuation starts from.
func applyTurn(t *Thread, in object, messageID string, attachments []Attachment) error {
	if t.messageIndex(messageID) >= 0 {
		return nil
	}
	messages := t.Messages
	kind := str(in["kind"])
	content, quote := str(in["content"]), str(in["quote"])
	if kind == "edit" || kind == "regenerate" || kind == "continue" {
		index := t.messageIndex(str(in["messageId"]))
		// A continuation names its reply or extends the last message.
		if kind == "continue" && str(in["messageId"]) == "" {
			index = len(messages) - 1
		}
		if index < 0 {
			return apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
		}
		role := "user"
		if kind != "edit" {
			role = "assistant"
		}
		if messages[index].Role != role {
			return apiErr(400, "BAD_REQUEST", "The turn kind does not match the message role")
		}
		if kind == "edit" {
			if _, present := in["attachments"]; !present {
				attachments = messages[index].Attachments
				if attachments == nil {
					attachments = []Attachment{}
				}
			}
			if _, present := in["quote"]; !present {
				quote = messages[index].Quote
			}
		}
		// As on the web client, the continued reply becomes the last message.
		if kind == "continue" {
			index++
		}
		messages = messages[:index]
	}
	if kind == "resolve" {
		found := false
		for i := range messages {
			for j := range messages[i].Timeline {
				if block := &messages[i].Timeline[j]; block.callID() == str(in["toolCallId"]) {
					block.Resolution, block.ResolvedAt = raw(in["resolution"]), raw(timestamp())
					found = true
				}
			}
		}
		if !found {
			return apiErr(404, "THREAD_NOT_FOUND", "The tool call was not found")
		}
		content = str(obj(in["resolution"])["text"])
	}
	if kind == "ask" {
		content, quote = str(in["quote"]), ""
	}
	if kind != "regenerate" && kind != "retryToolCall" && kind != "continue" {
		messages = append(messages, Message{ID: messageID, Role: "user", Content: content, Quote: quote, Attachments: attachments, CreatedAt: timestamp()})
	}
	t.Messages = messages
	return nil
}

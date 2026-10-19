package main

// applyTurn makes the change a turn's kind makes to a transcript before its
// reply: a new or edited question, a resolved tool call, or the cut a
// regeneration or continuation starts from.
func applyTurn(thread, in object, messageID string, attachments []any) error {
	messages := arr(thread["messages"])
	for _, m := range messages {
		if obj(m)["id"] == messageID {
			return nil
		}
	}
	kind := str(in["kind"])
	content, quote := str(in["content"]), in["quote"]
	if kind == "edit" || kind == "regenerate" || kind == "continue" {
		index := -1
		for i, m := range messages {
			if obj(m)["id"] == in["messageId"] {
				index = i
				break
			}
		}
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
		if obj(messages[index])["role"] != role {
			return apiErr(400, "BAD_REQUEST", "The turn kind does not match the message role")
		}
		if kind == "edit" {
			if _, present := in["attachments"]; !present {
				attachments = arr(obj(messages[index])["attachments"])
			}
			if _, present := in["quote"]; !present {
				quote = obj(messages[index])["quote"]
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
		for _, m := range messages {
			for _, b := range arr(obj(m)["timeline"]) {
				block := obj(b)
				if firstString(block["toolCallId"], block["id"]) == str(in["toolCallId"]) {
					block["resolution"], block["resolvedAt"] = in["resolution"], timestamp()
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
		content = str(in["quote"])
		quote = nil
	}
	if kind != "regenerate" && kind != "retryToolCall" && kind != "continue" {
		messages = append(messages, object{"id": messageID, "role": "user", "content": content, "quote": quote, "attachments": attachments, "createdAt": timestamp(), "timestamp": timestamp()})
	}
	thread["messages"] = messages
	return nil
}

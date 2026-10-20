package main

// errAlreadyResolved refuses a second answer to a widget, which another
// turn already resolved.
var errAlreadyResolved = apiErr(409, "ALREADY_RESOLVED", "The tool call was already resolved; reload the chat")

// applyTurn makes the change a turn's kind makes to a transcript before its
// reply: a new or edited question, a resolved tool call, or the cut a
// regeneration or continuation starts from. An answer to the last question
// changes nothing.
func applyTurn(t *Thread, in *turnRequest, messageID string, attachments []Attachment) error {
	if t.messageIndex(messageID) >= 0 {
		return nil
	}
	messages := t.Messages
	content, quote := valueOr(in.Content, ""), valueOr(in.Quote, "")
	if in.Kind == "edit" || in.Kind == "regenerate" || in.Kind == "continue" {
		index := t.messageIndex(in.MessageID)
		// A continuation names its reply or extends the last message.
		if in.Kind == "continue" && in.MessageID == "" {
			index = len(messages) - 1
		}
		if index < 0 {
			return apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
		}
		role := "user"
		if in.Kind != "edit" {
			role = "assistant"
		}
		if messages[index].Role != role {
			return apiErr(400, "BAD_REQUEST", "The turn kind does not match the message role")
		}
		if in.Kind == "edit" {
			if in.Attachments == nil {
				attachments = messages[index].Attachments
				if attachments == nil {
					attachments = []Attachment{}
				}
			}
			if in.Quote == nil {
				quote = messages[index].Quote
			}
		}
		// As on the web client, the continued reply becomes the last message.
		if in.Kind == "continue" {
			index++
		}
		messages = messages[:index]
	}
	if in.Kind == "resolve" {
		found := false
		for i := range messages {
			for j := range messages[i].Timeline {
				if block := &messages[i].Timeline[j]; block.callID() == in.ToolCallID {
					if block.resolved() {
						return errAlreadyResolved
					}
					block.Resolution, block.ResolvedAt = in.Resolution, raw(timestamp())
					found = true
				}
			}
		}
		if !found {
			return apiErr(404, "THREAD_NOT_FOUND", "The tool call was not found")
		}
		content, _ = in.resolution()
	}
	if in.Kind == "ask" {
		content, quote = valueOr(in.Quote, ""), ""
	}
	if in.Kind != "regenerate" && in.Kind != "retryToolCall" && in.Kind != "continue" && in.Kind != "answer" {
		messages = append(messages, Message{ID: messageID, Role: "user", Content: content, Quote: quote, Attachments: attachments, CreatedAt: timestamp()})
	}
	t.Messages = messages
	return nil
}

// retried is the turn a retry carries out, which the chat decides: a failed
// or stopped continuation continues again, any other last reply is
// regenerated, and a last question without a reply is answered.
func retried(t *Thread, in *turnRequest) (*turnRequest, error) {
	if len(t.Messages) == 0 {
		return nil, apiErr(400, "BAD_REQUEST", "The chat has nothing to retry")
	}
	turn, last := *in, t.Messages[len(t.Messages)-1]
	switch {
	case last.Role == "user":
		turn.Kind, turn.MessageID = "answer", ""
	case t.LastRequestKind == "continue" && (valueOr(last.IsError, false) || valueOr(last.IsInterrupted, false)):
		turn.Kind, turn.MessageID = "continue", last.ID
	default:
		turn.Kind, turn.MessageID = "regenerate", last.ID
	}
	return &turn, nil
}

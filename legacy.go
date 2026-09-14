package main

import "time"

func isoTime(value any) string {
	if s := str(value); s != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return parsed.UTC().Format(time.RFC3339Nano)
		}
		return s
	}
	if n := integer(value); n > 0 {
		return time.UnixMilli(int64(n)).UTC().Format(time.RFC3339Nano)
	}
	return ""
}

// Read old timeline variants once at the server boundary. The flat legacy
// fields remain on stored messages while iOS is still a writer.
func upgradeTimeline(message object) {
	if message["role"] != "assistant" {
		return
	}
	source := arr(message["timeline"])
	id := str(message["id"])
	if len(source) == 0 {
		search := obj(message["webSearch"])
		addSearch := func() {
			if len(search) > 0 {
				source = append(source, object{"type": "web_search", "id": id + "-search", "state": search})
			}
		}
		if boolean(message["webSearchBeforeThinking"], false) {
			addSearch()
		}
		if text := str(message["thoughts"]); text != "" {
			source = append(source, object{"type": "thinking", "id": id + "-thinking", "content": text, "duration": message["thinkingDuration"]})
		}
		if !boolean(message["webSearchBeforeThinking"], false) {
			addSearch()
		}
		if fetches := arr(message["urlFetches"]); len(fetches) > 0 {
			source = append(source, object{"type": "url_fetches", "id": id + "-fetches", "fetches": fetches})
		}
		if calls := arr(message["codeExecCalls"]); len(calls) > 0 {
			source = append(source, object{"type": "code_exec", "id": id + "-code", "calls": calls})
		}
		for _, item := range arr(message["toolCalls"]) {
			call := clone(obj(item))
			call["type"] = "tool_call"
			call["toolCallId"] = call["id"]
			source = append(source, call)
		}
		if text := str(message["content"]); text != "" {
			source = append(source, object{"type": "content", "id": id + "-content", "content": text})
		}
	}
	blocks := []any{}
	call := func(id, name, args string, result any) {
		blocks = append(blocks, object{"type": "tool_call", "id": id, "toolCallId": id, "name": name, "arguments": args, "result": result, "complete": true})
	}
	for index, item := range source {
		block := obj(item)
		blockID := firstString(block["id"], id+"-block-"+jsonNumber(index))
		switch block["type"] {
		case "web_search":
			state := obj(block["state"])
			results := []object{}
			for _, value := range arr(state["sources"]) {
				row := obj(value)
				results = append(results, object{"url": row["url"], "title": row["title"], "snippet": firstString(row["snippet"], row["description"])})
			}
			call(blockID, "web_search", string(raw(object{"query": str(state["query"])})), object{"query": str(state["query"]), "results": results})
		case "url_fetches":
			for i, value := range arr(block["fetches"]) {
				fetch := obj(value)
				call(firstString(fetch["id"], blockID+"-"+jsonNumber(i)), "web_fetch", string(raw(object{"url": fetch["url"]})), object{"url": fetch["url"], "title": str(fetch["title"]), "text": str(fetch["text"])})
			}
		case "code_exec":
			for i, value := range arr(block["calls"]) {
				c := obj(value)
				args := str(c["arguments"])
				if args == "" {
					args = string(raw(obj(c["arguments"])))
				}
				call(firstString(c["id"], blockID+"-"+jsonNumber(i)), str(c["toolName"]), args, normalizeToolResult(toolCall{name: str(c["toolName"]), args: args}, str(c["output"])))
			}
		default:
			block["id"] = blockID
			blocks = append(blocks, block)
		}
	}
	message["timeline"] = blocks
}

func upgradeAttachments(message object) {
	attachments := arr(message["attachments"])
	if len(attachments) == 0 {
		if content := firstString(message["documentContent"], message["multimodalText"]); content != "" {
			attachments = append(attachments, object{"type": "document", "fileName": "Attached document", "textContent": content})
		}
		for i, item := range arr(message["imageData"]) {
			image := obj(item)
			if str(image["base64"]) != "" {
				attachments = append(attachments, object{"type": "image", "fileName": "image-" + jsonNumber(i), "mimeType": firstString(image["mimeType"], "image/png"), "base64": image["base64"]})
			}
		}
	}
	for index, item := range attachments {
		attachment := obj(item)
		if str(attachment["id"]) == "" {
			attachment["id"] = str(message["id"]) + "-attachment-" + jsonNumber(index)
		}
	}
	if len(attachments) > 0 {
		message["attachments"] = attachments
	}
}

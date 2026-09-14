package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

//go:embed memory-schema.json
var memorySchema json.RawMessage

// Memory is opt-in at deployment and can be disabled for an individual project.
// The timestamp is per thread, so a later turn in one project chat cannot skip
// messages in another chat. Manual changes win over a concurrent extraction.
func (h *harness) extractMemory(ctx context.Context, run *chatRun, thread *storedRow) {
	if !h.memoryEnabled || str(thread.Data["projectId"]) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	projectID := str(thread.Data["projectId"])
	project, err := h.storeRows.load(ctx, run.principal, run.key, "project", projectID)
	if err != nil || !boolean(project.Data["memoryEnabled"], true) {
		return
	}
	catalog := h.catalog.get()
	if catalog == nil || catalog.MemoryPrompt == "" {
		return
	}
	var model *model
	for _, candidate := range h.models {
		if model == nil || candidate.name == "gpt-oss-120b" {
			model = candidate
		}
	}
	if model == nil {
		return
	}
	checkpoint := str(obj(project.Data["harnessMemoryCursors"])[thread.ID])
	facts := arr(project.Data["memory"])
	formattedFacts := []string{}
	for _, item := range facts {
		fact := obj(item)
		formattedFacts = append(formattedFacts, fmt.Sprintf("[id: %s] [%s] %s (confidence: %v, date: %s)", str(fact["id"]), str(fact["category"]), str(fact["fact"]), fact["confidence"], str(fact["date"])))
	}
	if len(formattedFacts) == 0 {
		formattedFacts = []string{"(No previous facts)"}
	}
	conversation, latest, users := []string{}, checkpoint, 0
	for _, item := range arr(thread.Data["messages"]) {
		message := obj(item)
		at := isoTime(firstString(message["timestamp"], message["createdAt"]))
		if at <= checkpoint || message["role"] != "user" && message["role"] != "assistant" {
			continue
		}
		content := str(message["content"])
		if message["role"] == "user" {
			if len(strings.Fields(content)) < 5 {
				continue
			}
			users++
		}
		role := "Assistant"
		if message["role"] == "user" {
			role = "User"
		}
		conversation = append(conversation, fmt.Sprintf("[%s] %s: %s", at, role, content))
		latest = max(latest, at)
	}
	if users == 0 {
		return
	}
	prompt := strings.ReplaceAll(catalog.MemoryPrompt, "{CURRENT_FACTS}", strings.Join(formattedFacts, "\n"))
	prompt = strings.ReplaceAll(prompt, "{CONVERSATION_CONTEXT}", strings.Join(conversation, "\n\n"))
	if tokenCount(prompt)+tokenCount(string(memorySchema))+256 > model.context*4/5 {
		return
	}
	output, err := h.completion(ctx, model, object{"model": model.name, "messages": []object{{"role": "system", "content": "Analyze conversation and manage user facts. Output valid JSON only."}, {"role": "user", "content": prompt}}, "response_format": object{"type": "json_schema", "json_schema": object{"name": "fact_operations", "schema": memorySchema}}})
	if err != nil {
		return
	}
	var result object
	if json.Unmarshal([]byte(output), &result) != nil || validateSchema(memorySchema, result) != nil {
		return
	}
	updated := applyFacts(facts, arr(result["operations"]))
	_, _ = h.mutate(ctx, run.principal, run.key, "project", projectID, false, nil, func(data object) error {
		if string(raw(data["memory"])) != string(raw(project.Data["memory"])) || str(obj(data["harnessMemoryCursors"])[thread.ID]) != checkpoint {
			return apiErr(409, "REVISION_CONFLICT", "Project memory changed during extraction")
		}
		data["memory"] = updated
		cursors := obj(data["harnessMemoryCursors"])
		cursors[thread.ID] = latest
		data["harnessMemoryCursors"], data["lastProcessedTimestamp"] = cursors, latest
		return nil
	})
}

func applyFacts(existing, operations []any) []any {
	facts := arr(clone(object{"facts": existing})["facts"])
	for _, item := range operations {
		op := obj(item)
		switch op["action"] {
		case "add":
			fact := selectFields(obj(op["fact"]), "fact date category confidence")
			fact["id"] = uuid.NewString()
			facts = append(facts, fact)
		case "update":
			for _, item := range facts {
				fact := obj(item)
				if fact["id"] == op["factId"] {
					for field, value := range selectFields(obj(op["updates"]), "fact category confidence") {
						fact[field] = value
					}
				}
			}
		case "delete":
			facts = slices.DeleteFunc(facts, func(value any) bool { return obj(value)["id"] == op["factId"] })
		}
	}
	if len(facts) > 500 {
		slices.SortStableFunc(facts, func(a, b any) int { return strings.Compare(str(obj(b)["date"]), str(obj(a)["date"])) })
		facts = facts[:500]
	}
	return facts
}

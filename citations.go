package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"strings"
)

// Search and fetch results as the model reads them, ported from the model
// router (toolruntime/citations/format.go) without its Harmony cursors: every
// model here is asked for markdown links.

const citationInstructions = "Attach sources by embedding a clickable markdown link to the original URL directly after the sentence it supports. Format every citation exactly like this example, copying the punctuation characters verbatim: The sky is blue [Example page](https://example.com/article). Reference 1-2 sources per claim; do not reference every source on every sentence. Copy the URL character-for-character from the tool output: preserve or omit a trailing slash exactly as the tool emitted it, keep query parameters verbatim, and do not append punctuation, whitespace, zero-width characters, or any other character after the URL before the closing parenthesis. Never invent URLs, never paraphrase URLs, and never wrap the link in any other brackets, braces, or quotation marks."

// modelCopy is a search or fetch result as cited text; other output is read as it is.
func modelCopy(name, content string) string {
	var value object
	if json.Unmarshal([]byte(content), &value) != nil || len(value) == 0 {
		return content
	}
	formatted := ""
	switch name {
	case "web_search":
		formatted = formatSearchOutput(value["results"])
	case "web_fetch":
		formatted = cmp.Or(formatFetchOutput(value["pages"]), formatFetchFailures(value["results"]))
	}
	return cmp.Or(formatted, content)
}

func formatSearchOutput(raw any) string {
	results, _ := raw.([]any)
	if len(results) == 0 {
		return "No safe search results were found."
	}
	var out strings.Builder
	for _, item := range results {
		result, ok := item.(map[string]any)
		if !ok {
			continue
		}
		fmt.Fprintf(&out, "Source: %s\n", cmp.Or(strings.TrimSpace(str(result["title"])), "Search result"))
		if url := strings.TrimSpace(str(result["url"])); url != "" {
			fmt.Fprintf(&out, "URL: %s\n", url)
		}
		if published := strings.TrimSpace(str(result["published_date"])); published != "" {
			fmt.Fprintf(&out, "Published: %s\n", published)
		}
		if content := strings.TrimSpace(str(result["content"])); content != "" {
			out.WriteString(content + "\n")
		}
		out.WriteString("\n")
	}
	return strings.TrimSpace(out.String())
}

// formatFetchOutput is empty when nothing was fetched.
func formatFetchOutput(raw any) string {
	var out strings.Builder
	for _, item := range arr(raw) {
		page, ok := item.(map[string]any)
		if !ok {
			continue
		}
		out.WriteString("Source: Fetched page\n")
		if url := strings.TrimSpace(str(page["url"])); url != "" {
			fmt.Fprintf(&out, "URL: %s\n", url)
		}
		if content := strings.TrimSpace(str(page["content"])); content != "" {
			out.WriteString(content + "\n")
		}
		out.WriteString("\n")
	}
	return strings.TrimSpace(out.String())
}

func formatFetchFailures(raw any) string {
	var lines []string
	for _, item := range arr(raw) {
		result, ok := item.(map[string]any)
		if !ok || strings.TrimSpace(str(result["status"])) == "completed" {
			continue
		}
		url, failure := strings.TrimSpace(str(result["url"])), strings.TrimSpace(str(result["error"]))
		switch {
		case url != "" && failure != "":
			lines = append(lines, fmt.Sprintf("Fetch failed for %s: %s", url, failure))
		case url != "":
			lines = append(lines, fmt.Sprintf("Fetch failed for %s.", url))
		case failure != "":
			lines = append(lines, "Fetch failed: "+failure)
		}
	}
	return strings.Join(lines, "\n")
}

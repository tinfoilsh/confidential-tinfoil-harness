// Package summarize writes the summaries the harness uses, each for a Task:
// a chat's title, a label of what a reply is reasoning about, and the
// summary that stands in for a compacted chat's older messages. A task names
// the model role it runs on, how its input is framed, what its summary holds
// and how that is checked. A title or label runs on the harness's own
// account, so it bills no caller; a compaction bills the caller whose run
// asks for it.
package summarize

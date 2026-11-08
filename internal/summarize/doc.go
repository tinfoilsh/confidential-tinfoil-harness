// Package summarize writes the summaries the harness uses, each for a Task:
// a chat's title, a label of what a reply is reasoning about, the summary
// that stands in for a compacted chat's older messages, and the notes that
// stand in for what a research read. A task names the model role it runs
// on, how its input is framed, what its summary holds and how that is
// checked. A title or label runs on the harness's own account, so it bills
// no caller; a compaction or research notes bill the caller whose run asks
// for them.
package summarize

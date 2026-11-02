// Package summarize writes the summaries the harness uses, each for a Task:
// a chat's title, and a label of what a reply is reasoning about. A task names
// the model role it runs on, how its input is framed, what its summary holds
// and how that is checked. Every summary runs on the harness's own account,
// so none bills a caller.
package summarize

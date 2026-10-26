// Package engine carries out turns: it admits each as its chat's running
// reply or queues it behind one, builds the prompt, streams the model's
// reply and runs its tool calls, commits what the run produced, screens it,
// and holds the live runs of this instance for clients to follow, cancel and
// queue behind. It serves its own routes.
package engine

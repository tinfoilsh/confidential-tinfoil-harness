package api

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

// Rescued logs a panic and turns it into an error worth surfacing. Without one
// of these on every goroutine the harness spawns, a panic in any single run
// takes the process, and with it every other run this enclave holds in memory.
// The panic value may contain plaintext, so only the stack is logged.
//
// It takes the recovered value rather than recovering itself, because recover
// works only in the function a defer names, which is one frame above this.
func Rescued(what string, p any) error {
	if p == nil {
		return nil
	}
	slog.Error("recovered a panic", "in", what, "stack", string(debug.Stack()))
	return fmt.Errorf("%s failed", what)
}

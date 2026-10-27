// Command confidential-tinfoil-harness runs Tinfoil Chat conversations whose
// model turns and tool calls all go to enclaves it attests against a pinned repo.
package main

import "github.com/tinfoilsh/confidential-tinfoil-harness/internal/server"

func main() {
	server.Run()
}

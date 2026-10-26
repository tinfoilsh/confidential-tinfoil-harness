package main

import (
	"fmt"
	"regexp"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// maxReverseMillis is the instant a reverse-time chat id counts down from.
const maxReverseMillis = 9999999999999

// rowIDPattern is the id a client gives a chat it creates, as rowID makes it.
var rowIDPattern = regexp.MustCompile(`^\d{13}_[0-9a-f-]{36}$`)

func rowID() string {
	return fmt.Sprintf("%013d_%s", maxReverseMillis-api.Now().UnixMilli(), uuid.NewString())
}

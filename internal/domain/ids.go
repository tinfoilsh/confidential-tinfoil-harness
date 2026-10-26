package domain

import (
	"fmt"
	"regexp"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// MaxReverseMillis is the instant a reverse-time chat id counts down from.
const MaxReverseMillis = 9999999999999

// RowIDPattern is the id a client gives a chat it creates, as rowID makes it.
var RowIDPattern = regexp.MustCompile(`^\d{13}_[0-9a-f-]{36}$`)

func RowID() string {
	return fmt.Sprintf("%013d_%s", MaxReverseMillis-api.Now().UnixMilli(), uuid.NewString())
}

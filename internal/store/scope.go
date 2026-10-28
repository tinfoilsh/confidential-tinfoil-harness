package store

import (
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

// keyID is the hex id a content key is registered and sealed under.
func keyID(key auth.ContentKey) string {
	id, _ := seal.DeriveKeyID(key[:])
	return seal.KeyIDHex(id)
}

// maxResourceID bounds the ids a grant names.
const maxResourceID = 256

// runIDValid is whether id can name a chat, project or document: one path
// segment the control plane stores it under.
func runIDValid(id string) bool {
	return id != "" && id != "." && id != ".." && len(id) <= maxResourceID && !strings.ContainsAny(id, "/\x00")
}

// A storage operation, as a grant allows or refuses it.
type storageOp int

const (
	opPush storageOp = iota
	opPull
	opList
	opRemove
	opPutAttachment
	opGetAttachment
	opImport
)

// grantScope is what a storage grant lets its holder do: a run reads and
// writes its chat and reads its project's context; an operation does what
// it is for and nothing else.
type grantScope auth.StorageGrant

func (g *grantScope) allows(op storageOp) bool {
	if !api.Now().Before(g.ExpiresAt) {
		return false
	}
	switch g.Operation {
	case "project-upload":
		return op == opPull || op == opPush
	case "import":
		return op == opImport
	case "export":
		return op == opPull || op == opList || op == opGetAttachment
	case "":
		return op == opPush || op == opPull || op == opList || op == opPutAttachment || op == opGetAttachment
	}
	return false
}

func (g *grantScope) keyMatches(key auth.ContentKey) bool { return keyID(key) == g.KeyID }

// ownsDocument is whether id names a document of the grant's project.
func (g *grantScope) ownsDocument(id string) bool {
	if g.ProjectID == "" {
		return false
	}
	document, ok := strings.CutPrefix(id, g.ProjectID+"/")
	return ok && runIDValid(document)
}

func (g *grantScope) permitsPull(key auth.ContentKey, scope string, ids []string) bool {
	if !g.allows(opPull) || !g.keyMatches(key) {
		return false
	}
	if g.Operation == "export" {
		return seal.Scope(scope).Valid()
	}
	if len(ids) == 0 {
		return false
	}
	if g.Operation == "project-upload" {
		for _, id := range ids {
			if scope == "project" && id == g.ProjectID || scope == "project_document" && g.ownsDocument(id) {
				continue
			}
			return false
		}
		return true
	}
	for _, id := range ids {
		switch scope {
		case "chat":
			if id != g.ChatID {
				return false
			}
		case "profile":
			if id != seal.ProfileSingletonID {
				return false
			}
		case "project":
			if g.ProjectID == "" || id != g.ProjectID {
				return false
			}
		case "project_document":
			if !g.ownsDocument(id) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func (g *grantScope) permitsPush(key auth.ContentKey, scope string, row *domain.StoredRow) bool {
	if !g.allows(opPush) || !g.keyMatches(key) {
		return false
	}
	if g.Operation == "project-upload" {
		return scope == "project_document" && g.ownsDocument(row.ID) && row.ETag == "" && row.Project != nil && *row.Project == g.ProjectID
	}
	// The grant owns the chat, including its metadata. ProjectID limits
	// context reads; the user can move the chat while a reply is running.
	return g.Operation == "" && scope == "chat" && row.ID == g.ChatID
}

// permitsList is whether the grant may list a scope's rows, or those of a
// project: an export lists anything, a run only its project's documents.
func (g *grantScope) permitsList(scope, project string) bool {
	if !g.allows(opList) {
		return false
	}
	return g.Operation == "export" || scope == "project_document" && project != "" && project == g.ProjectID
}

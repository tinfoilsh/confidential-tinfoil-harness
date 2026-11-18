package store

import (
	"context"
	"errors"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A document's write names the file it keeps, so the purge sweep deletes
// a version's file once a newer version replaces it, and the last file
// once the document is deleted, while the file the document names stays
// readable under the key its row seals.
func TestADocumentKeepsOnlyTheFileItsLatestWriteNames(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.push(t, "project", "trip", `{"name":"Trip"}`)
	upload := func(document string, replace bool, data string) domain.ProjectDocument {
		t.Helper()
		replaced := ""
		if replace {
			replaced = document
		}
		p := *h.p
		var err error
		if p.Grant, err = h.rows.AuthorizeStorage(ctx, h.p, h.key, "project-upload", "trip", replaced); err != nil {
			t.Fatal(err)
		}
		file, err := h.store.PutDocumentFile(ctx, &p, "trip", document, []byte(data))
		if err != nil {
			t.Fatalf("store %s's file: %v", document, err)
		}
		doc, err := h.store.SaveProjectDocument(ctx, &p, h.key, domain.ProjectDocument{ID: document, ProjectID: "trip", Filename: "plan.md", ContentType: "text/markdown", SizeBytes: len(data), Content: data, File: file}, replace)
		if err != nil {
			t.Fatalf("save %s: %v", document, err)
		}
		return doc
	}
	read := func(document string) string {
		t.Helper()
		doc, err := h.store.ProjectDocument(ctx, h.p, h.key, "trip", document)
		if err != nil {
			t.Fatalf("load %s: %v", document, err)
		}
		data, err := h.store.DocumentFile(ctx, h.p, &doc)
		if err != nil {
			t.Fatalf("read %s's file: %v", document, err)
		}
		return string(data)
	}

	first := upload("plan", false, "Day one")
	second := upload("plan", true, "Day one and two")
	if second.CreatedAt != first.CreatedAt || read("plan") != "Day one and two" {
		t.Fatalf("the new version was added %q (first %q) and reads %q", second.CreatedAt, first.CreatedAt, read("plan"))
	}
	if deleted, err := h.store.purgeUnreferenced(ctx); deleted != 1 || err != nil || h.bk.Has(first.File.ID) || !h.bk.Has(second.File.ID) {
		t.Fatalf("sweep = (%d, %v); buckets holds the first version=%v, the second=%v", deleted, err, h.bk.Has(first.File.ID), h.bk.Has(second.File.ID))
	}
	if read("plan") != "Day one and two" {
		t.Fatal("the sweep took the file the document names")
	}

	// A replacement may store a file for its own document alone.
	p := *h.p
	p.Grant, _ = h.rows.AuthorizeStorage(ctx, h.p, h.key, "project-upload", "trip", "plan")
	if _, err := h.store.PutDocumentFile(ctx, &p, "trip", "other", []byte("x")); !errors.Is(err, errRunScope) {
		t.Fatalf("a replacement of plan stored a file for another document: %v", err)
	}

	if err := h.store.RemoveRow(ctx, h.p, h.key, "project_document", "trip/plan"); err != nil {
		t.Fatal(err)
	}
	if deleted, err := h.store.purgeUnreferenced(ctx); deleted != 1 || err != nil || h.bk.Has(second.File.ID) {
		t.Fatalf("sweep after the delete = (%d, %v); buckets still holds the file=%v", deleted, err, h.bk.Has(second.File.ID))
	}
}

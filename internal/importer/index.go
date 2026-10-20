package importer

import (
	"path"
	"slices"
	"strings"
)

// Index looks up the entries of an archive, so an attachment can only name
// an entry the archive holds. Its caller builds it from entry names it has
// already found safe.
type Index struct {
	byName map[string]struct{}
	byBase map[string]string
	// byStem finds the entries whose basename less its extension is a file
	// id.
	byStem map[string]idMatch
	// sorted holds each entry once, ordered by basename, so the entries
	// whose basename is a file id followed by a separator and more form a
	// range found by binary search.
	sorted []indexEntry
}

type indexEntry struct{ base, name string }

// idMatch is the first entry a file id finds and how many it finds.
type idMatch struct {
	name  string
	count int
}

func (m idMatch) unique() (string, bool) {
	if m.count != 1 {
		return "", false
	}
	return m.name, true
}

// addID records that id finds name.
func addID(ids map[string]idMatch, id, name string) {
	m := ids[id]
	if m.count == 0 {
		m.name = name
	}
	m.count++
	ids[id] = m
}

// idSeparators may follow a file id in a media file's name.
const idSeparators = ".-_"

// NewIndex indexes entry names. The first entry with a basename claims it,
// which is deterministic for a given archive.
func NewIndex(names []string) *Index {
	idx := &Index{byName: make(map[string]struct{}, len(names)), byBase: make(map[string]string, len(names)), byStem: map[string]idMatch{}}
	for _, n := range names {
		clean := normalizeEntry(n)
		if clean == "" {
			continue
		}
		if _, ok := idx.byName[clean]; ok {
			continue
		}
		idx.byName[clean] = struct{}{}
		base := path.Base(clean)
		if idx.byBase[base] == "" {
			idx.byBase[base] = clean
		}
		addID(idx.byStem, strings.TrimSuffix(base, path.Ext(base)), clean)
		idx.sorted = append(idx.sorted, indexEntry{base: base, name: clean})
	}
	slices.SortFunc(idx.sorted, func(a, b indexEntry) int { return strings.Compare(a.base, b.base) })
	return idx
}

// byPrefix finds the entries whose basename is id followed by a separator
// and more, counting no further than a second.
func (i *Index) byPrefix(id string) idMatch {
	var m idMatch
	for _, sep := range idSeparators {
		prefix := id + string(sep)
		k, _ := slices.BinarySearchFunc(i.sorted, prefix, func(e indexEntry, p string) int { return strings.Compare(e.base, p) })
		for ; k < len(i.sorted) && m.count < 2 && strings.HasPrefix(i.sorted[k].base, prefix); k++ {
			if m.count == 0 {
				m.name = i.sorted[k].name
			}
			m.count++
		}
	}
	return m
}

func normalizeEntry(name string) string {
	return strings.TrimPrefix(path.Clean("/"+strings.ReplaceAll(name, "\\", "/")), "/")
}

// Exact finds the entry a reference names by its path.
func (i *Index) Exact(ref string) (string, bool) {
	if i == nil {
		return "", false
	}
	clean := normalizeEntry(ref)
	if _, ok := i.byName[clean]; ok && clean != "" {
		return clean, true
	}
	return "", false
}

// Basename finds the entry a bare file name names.
func (i *Index) Basename(ref string) (string, bool) {
	if i == nil {
		return "", false
	}
	clean := normalizeEntry(ref)
	if clean == "" {
		return "", false
	}
	name, ok := i.byBase[path.Base(clean)]
	return name, ok
}

// ByIDPrefix finds the one entry whose basename is a file id, or starts
// with it, as ChatGPT and Claude name media files.
func (i *Index) ByIDPrefix(id string) (string, bool) {
	id = strings.TrimSpace(id)
	if i == nil || id == "" {
		return "", false
	}
	if name, ok := i.byBase[id]; ok {
		return name, true
	}
	if name, ok := i.byStem[id].unique(); ok {
		return name, true
	}
	return i.byPrefix(id).unique()
}

// imageTypes are the image types an export's file names imply.
var imageTypes = map[string]string{
	".png":  "image/png",
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".gif":  "image/gif",
	".webp": "image/webp",
	".bmp":  "image/bmp",
	".svg":  "image/svg+xml",
}

// isImage tells by a declared type or a file name whether a reference
// names an image. Its bytes are checked when they are read.
func isImage(mimeType, name string) bool {
	if strings.HasPrefix(strings.ToLower(mimeType), "image/") {
		return true
	}
	_, ok := imageTypes[strings.ToLower(path.Ext(name))]
	return ok
}

func imageType(name string) string { return imageTypes[strings.ToLower(path.Ext(name))] }

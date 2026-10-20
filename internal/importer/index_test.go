package importer

import "testing"

// A file id finds the one entry it names: its whole basename, its basename
// less the extension, or the start of its basename before a separator. An
// id that names several entries at the first of these that it matches
// finds none.
func TestByIDPrefixFindsOnlyOneEntry(t *testing.T) {
	index := NewIndex([]string{
		"p/shared.png", "q/shared.png",
		"img/file-a.png", "img/file-a-thumb.png",
		"x/file-b.png", "y/file-b.jpg",
		"file-c_photo.png",
		"file-d-1.png", "file-d_2.png",
		"file-ex.png",
		"dup/file-f.png", "dup/file-f.png",
	})
	for _, tc := range []struct {
		id, want string
	}{
		{"shared.png", "p/shared.png"},
		{"file-a", "img/file-a.png"},
		{"file-b", ""},
		{"file-c", "file-c_photo.png"},
		{"file-d", ""},
		{"file-e", ""},
		{"file-f", "dup/file-f.png"},
	} {
		if got, ok := index.ByIDPrefix(tc.id); got != tc.want || ok != (tc.want != "") {
			t.Errorf("%s found %q, %v; want %q", tc.id, got, ok, tc.want)
		}
	}
}

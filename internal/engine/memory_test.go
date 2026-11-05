package engine

import (
	"strings"
	"testing"
)

// /proc/meminfo gives kibibytes, and a kernel without MemAvailable must not
// read as having no memory left, which would refuse every turn.
func TestMeminfoIsReadInBytesAndNeedsMemAvailable(t *testing.T) {
	available, total, err := readMeminfo(strings.NewReader("MemTotal:       65536000 kB\nMemFree:         1024000 kB\nMemAvailable:   32768000 kB\nBuffers:          102400 kB\n"))
	if err != nil || available != 32768000*1024 || total != 65536000*1024 {
		t.Fatalf("read %d of %d bytes available (%v)", available, total, err)
	}
	if _, _, err := readMeminfo(strings.NewReader("MemTotal:       65536000 kB\nMemFree:         1024000 kB\n")); err == nil {
		t.Fatal("meminfo without MemAvailable was read")
	}
}

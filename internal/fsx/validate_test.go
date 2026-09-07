package fsx

import (
	"path"
	"strings"
	"testing"

	"esync/internal/fault"
)

// T-PATH-01..07: the traversal / absolute / NUL / reserved / length cases.
func TestValidatePath(t *testing.T) {
	long := strings.Repeat("a", 300)
	cases := []struct {
		name string
		p    string
		plat Platform
		want fault.Code
	}{
		{"ok simple", "a/b/c.txt", PlatformLinux, ""},
		{"ok dotfile", "a/.config/x", PlatformLinux, ""},
		{"empty", "", PlatformLinux, fault.E7010},
		{"nul", "a\x00b", PlatformLinux, fault.E7010},
		{"absolute", "/etc/passwd", PlatformLinux, fault.E7010},
		{"drive", "C:\\Windows", PlatformWindows, fault.E7010},
		{"unc", "\\\\server\\share", PlatformWindows, fault.E7010},
		{"parent", "../../etc", PlatformLinux, fault.E7010},
		{"embedded parent", "a/../../b", PlatformLinux, fault.E7010},
		{"dot", "a/./b", PlatformLinux, fault.E7010},
		{"esync", "a/.esync/x", PlatformLinux, fault.E7010},
		{"backslash parent", "a\\..\\..\\b", PlatformLinux, fault.E7010},
		{"reserved win", "docs/CON", PlatformWindows, fault.E7010},
		{"reserved win ext", "docs/nul.txt", PlatformWindows, fault.E7010},
		{"reserved ok on linux", "docs/CON", PlatformLinux, ""},
		{"reserved com9", "COM9", PlatformWindows, fault.E7010},
		{"component too long", long + "/x", PlatformLinux, fault.E7014},
		{"total too long win", "a/" + strings.Repeat("b/", 200) + "c", PlatformWindows, fault.E7014},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidatePath([]byte(c.p), c.plat)
			if got := fault.GetCode(err); got != c.want {
				t.Fatalf("ValidatePath(%q) code = %q, want %q (err=%v)", c.p, got, c.want, err)
			}
		})
	}
}

// Z-PATH-01: ValidatePath never accepts a path that would resolve outside the
// destination root.
func FuzzValidatePath(f *testing.F) {
	for _, s := range []string{
		"a/b", "../x", "a/../../b", "/abs", "C:\\x", "a\x00b", ".esync/x",
		"CON", "a/./b", strings.Repeat("z", 5000), "\\\\srv\\s", "a\\..\\b",
	} {
		f.Add([]byte(s), uint8(0))
	}
	f.Fuzz(func(t *testing.T, p []byte, plat uint8) {
		if err := ValidatePath(p, Platform(int(plat)%3)); err != nil {
			return
		}
		s := string(p)
		if s == "" {
			t.Fatal("accepted empty path")
		}
		for _, b := range p {
			if b == 0 {
				t.Fatal("accepted NUL byte")
			}
		}
		for _, c := range strings.FieldsFunc(s, func(r rune) bool { return r == '/' || r == '\\' }) {
			if c == "." || c == ".." || c == ".esync" {
				t.Fatalf("accepted forbidden component in %q", s)
			}
		}
		cleaned := path.Clean("/root/" + s)
		if cleaned != "/root" && !strings.HasPrefix(cleaned, "/root/") {
			t.Fatalf("accepted path escapes root: %q -> %q", s, cleaned)
		}
	})
}

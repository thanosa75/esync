package fsx

import (
	"testing"

	"esync/internal/fault"
)

// §12.5 / REQ-FS-044: a second distinct source path mapping onto an occupied
// platform-effective name → E7012.
func TestCollisions(t *testing.T) {
	ci := NewCollisions(PlatformWindows)
	if err := ci.Add("Docs/Report.txt"); err != nil {
		t.Fatal(err)
	}
	if err := ci.Add("docs/report.txt"); fault.GetCode(err) != fault.E7012 {
		t.Fatalf("case collision: got %v, want E7012", err)
	}
	// re-adding the identical path is not a collision
	if err := ci.Add("Docs/Report.txt"); err != nil {
		t.Fatalf("identical path re-add: %v", err)
	}

	cs := NewCollisions(PlatformLinux)
	if err := cs.Add("Docs/Report.txt"); err != nil {
		t.Fatal(err)
	}
	if err := cs.Add("docs/report.txt"); err != nil {
		t.Fatalf("case-sensitive dest should not collide: %v", err)
	}

	// macOS normalises: NFC and NFD spellings of the same name collide.
	mac := NewCollisions(PlatformDarwin)
	if err := mac.Add("café"); err != nil {
		t.Fatal(err)
	}
	if err := mac.Add("café"); fault.GetCode(err) != fault.E7012 {
		t.Fatalf("NFD/NFC collision on macOS: got %v, want E7012", err)
	}
}

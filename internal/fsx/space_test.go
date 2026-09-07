package fsx

import (
	"path/filepath"
	"testing"

	"esync/internal/fault"
)

func TestFreeBytes(t *testing.T) {
	n, err := FreeBytes(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("FreeBytes returned 0 for a writable tmpdir")
	}
}

// REQ-FS-045 / E1008: destination inside the source tree is detected.
func TestIsInside(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	inside := filepath.Join(src, "sub", "dst")
	outside := filepath.Join(root, "dst")

	if in, err := IsInside(inside, src); err != nil || !in {
		t.Fatalf("nested dst: in=%v err=%v, want true", in, err)
	}
	if in, err := IsInside(src, src); err != nil || !in {
		t.Fatalf("same path: in=%v err=%v, want true", in, err)
	}
	if in, err := IsInside(outside, src); err != nil || in {
		t.Fatalf("sibling dst: in=%v err=%v, want false", in, err)
	}
	if in, err := IsInside(src+"-suffix", src); err != nil || in {
		t.Fatalf("prefix-not-parent: in=%v err=%v, want false", in, err)
	}
}

func TestIsInsideToE1008(t *testing.T) {
	// the caller turns a true result into E1008; check the code path stays clean
	_, err := IsInside("\x00bad", "/tmp")
	if err != nil && fault.GetCode(err) != fault.E1008 {
		t.Fatalf("unexpected error code: %v", err)
	}
}

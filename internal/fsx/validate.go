// Package fsx is esync's platform filesystem layer (ARCHITECTURE §12.2, §12.3,
// §12.5): path validation, safe rooted IO, atomic publish, metadata, the
// free-space guard, the self-copy guard, and the resume journal.
package fsx

import (
	"errors"
	"strings"

	"esync/internal/fault"
)

// Platform is the destination operating system, which decides path limits and
// reserved names (§12.5).
type Platform int

const (
	PlatformLinux Platform = iota
	PlatformDarwin
	PlatformWindows
)

type pathLimits struct{ component, total int }

func limitsFor(p Platform) pathLimits {
	switch p {
	case PlatformWindows:
		return pathLimits{component: 255, total: 260}
	case PlatformDarwin:
		return pathLimits{component: 255, total: 1024}
	default:
		return pathLimits{component: 255, total: 4096}
	}
}

// ValidatePath checks a manifest path BEFORE any filesystem call (§12.5). Every
// rejection is E7010 except an over-length path or component, which is E7014.
func ValidatePath(p []byte, destPlatform Platform) error {
	reject := func(msg string) error {
		return fault.New(fault.E7010, "validate path", string(p), errors.New(msg))
	}

	if len(p) == 0 {
		return reject("empty path")
	}
	for _, b := range p {
		if b == 0 {
			return reject("NUL byte in path")
		}
	}

	s := string(p)
	if strings.HasPrefix(s, `\\`) {
		return reject("UNC path")
	}
	if strings.HasPrefix(s, "/") || strings.HasPrefix(s, `\`) {
		return reject("absolute path")
	}
	if len(s) >= 2 && s[1] == ':' && isASCIILetter(s[0]) {
		return reject("drive-letter path")
	}

	limits := limitsFor(destPlatform)
	comps := strings.FieldsFunc(s, func(r rune) bool { return r == '/' || r == '\\' })
	for _, c := range comps {
		switch c {
		case ".", "..":
			return reject("dot component")
		case ".esync":
			return reject(".esync component")
		}
		if destPlatform == PlatformWindows && isWindowsReserved(c) {
			return reject("Windows-reserved name")
		}
		if len(c) > limits.component {
			return fault.New(fault.E7014, "validate path", s, errors.New("path component too long"))
		}
	}
	if len(s) > limits.total {
		return fault.New(fault.E7014, "validate path", s, errors.New("path too long for destination platform"))
	}
	return nil
}

func isASCIILetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isWindowsReserved(c string) bool {
	name := strings.ToUpper(c)
	if i := strings.IndexByte(name, '.'); i >= 0 {
		name = name[:i]
	}
	name = strings.TrimRight(name, " ")
	switch name {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}
	if len(name) == 4 && name[3] >= '1' && name[3] <= '9' &&
		(strings.HasPrefix(name, "COM") || strings.HasPrefix(name, "LPT")) {
		return true
	}
	return false
}

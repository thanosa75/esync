package fsx

import (
	"strings"

	"golang.org/x/text/unicode/norm"

	"esync/internal/fault"
)

// Collisions tracks destination paths already materialised this session, keyed by
// the platform-effective name: case-folded on a case-insensitive destination,
// NFC-normalised where the platform normalises (§12.5, REQ-FS-044). A second
// distinct source path mapping onto an occupied effective name is E7012.
type Collisions struct {
	caseInsensitive bool
	normalise       bool
	seen            map[string]string
}

// NewCollisions builds the set for a destination platform. macOS is treated as
// case-insensitive and NFC-normalising; Windows as case-insensitive.
func NewCollisions(destPlatform Platform) *Collisions {
	return &Collisions{
		caseInsensitive: destPlatform == PlatformWindows || destPlatform == PlatformDarwin,
		normalise:       destPlatform == PlatformDarwin,
		seen:            make(map[string]string),
	}
}

func (c *Collisions) effective(relPath string) string {
	e := relPath
	if c.normalise {
		e = norm.NFC.String(e)
	}
	if c.caseInsensitive {
		e = strings.ToLower(e)
	}
	return e
}

// Add records relPath. If a different source path already occupies the same
// effective name it returns E7012 and does not overwrite the first.
func (c *Collisions) Add(relPath string) error {
	eff := c.effective(relPath)
	if first, ok := c.seen[eff]; ok && first != relPath {
		return fault.Newf(fault.E7012, "destination name collision", relPath, nil,
			"collides with %q after platform normalisation", first)
	}
	c.seen[eff] = relPath
	return nil
}

//go:build !linux && !darwin

package digest

import "io/fs"

// statKey has no portable stat source on this platform, so the digest cache is
// simply never consulted here (ARCHITECTURE §11.3, advisory only).
func statKey(fs.FileInfo, Algo) (CacheKey, bool) { return CacheKey{}, false }

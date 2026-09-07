// Package digest is esync's content-digest layer (ARCHITECTURE §11): a small
// algorithm registry, resumable streaming-hash state, an advisory on-disk digest
// cache, and a constant-memory file hasher.
package digest

import (
	"crypto/md5"
	"crypto/sha256"
	"hash"
	"strings"

	"esync/internal/fault"
)

// Algo identifies a content-digest algorithm. It is wire-encoded as a u8
// (ARCHITECTURE §9.3 SESSION_PARAMS.hash_alg).
type Algo uint8

const (
	// MD5 is the default (REQ-HASH-002).
	MD5 Algo = 0
	// BLAKE3 is reserved: selecting it is a catalogued error in this build,
	// because adding a BLAKE3 implementation would mean a new dependency.
	BLAKE3 Algo = 1
	// SHA256 is the collision-resistant option.
	SHA256 Algo = 2
)

func (a Algo) String() string {
	switch a {
	case MD5:
		return "md5"
	case BLAKE3:
		return "blake3"
	case SHA256:
		return "sha256"
	default:
		return "unknown"
	}
}

// Parse maps a --hash flag value (case-insensitive) to an Algo. An unknown value
// is an E1007 fault (flag value outside its documented range).
func Parse(s string) (Algo, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "md5":
		return MD5, nil
	case "blake3":
		return BLAKE3, nil
	case "sha256", "sha-256":
		return SHA256, nil
	default:
		return 0, fault.Newf(fault.E1007, "parse hash algorithm", s, nil,
			"unknown hash %q; want one of md5, blake3, sha256", s)
	}
}

// New returns a fresh hash.Hash for the algorithm. BLAKE3 and any unknown value
// return an E1007 fault; no BLAKE3 dependency is pulled in (ARCHITECTURE §11).
func New(a Algo) (hash.Hash, error) {
	switch a {
	case MD5:
		return md5.New(), nil
	case SHA256:
		return sha256.New(), nil
	case BLAKE3:
		return nil, fault.Newf(fault.E1007, "select hash algorithm", a.String(), nil,
			"blake3 is reserved and not implemented in this build; use md5 or sha256")
	default:
		return nil, fault.Newf(fault.E1007, "select hash algorithm", a.String(), nil,
			"unknown hash algorithm %d", uint8(a))
	}
}

// Len is the digest byte length for the algorithm: 16 for MD5, 32 for
// SHA-256/BLAKE3, 0 for an unknown value (ARCHITECTURE §9.3 ManifestEntry).
func Len(a Algo) int {
	switch a {
	case MD5:
		return 16
	case BLAKE3, SHA256:
		return 32
	default:
		return 0
	}
}

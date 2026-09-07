// Package plan turns the sender walker's raw entries into the ordered, grouped,
// digested transfer plan (ARCHITECTURE §10). It is deliberately PURE: it imports
// no network and no filesystem-write package, so the determinism property test
// (P-SCAN-01, P-SCAN-02) is cheap to run.
package plan

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"slices"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Type is the kind of a plan entry. Directories and symlinks occupy file ids and
// group slots exactly like regular files (§10.4).
type Type uint8

const (
	TypeFile Type = iota
	TypeDir
	TypeSymlink
)

// Flags is the per-entry flag bitmask carried from the walker (§10.1).
type Flags uint32

const (
	// FlagHasHardlinkKey marks an entry whose HardlinkKey is meaningful.
	FlagHasHardlinkKey Flags = 1 << iota
	// FlagNameNonUTF8 marks an entry whose final name is not valid UTF-8; its
	// ordering key is the raw bytes, not NFC-normalised (§10.2).
	FlagNameNonUTF8
	// FlagSparseHint marks a file the walker believes is sparse.
	FlagSparseHint
	// FlagExecutable marks a file with any execute bit set.
	FlagExecutable
)

// Entry is one in-memory walker record. RelPath is the raw source path bytes with
// the OS separator; it travels unchanged so the receiver can reproduce the exact
// name where its platform allows (§10.2).
type Entry struct {
	RelPath     []byte
	Type        Type
	Flags       Flags
	Size        int64
	Mode        os.FileMode
	MtimeSec    int64
	MtimeNsec   uint32
	Dev         uint64
	Ino         uint64
	LinkTarget  []byte
	HardlinkKey uint64
}

const (
	groupSize       = 1024
	protocolVersion = 1
	hashAlg         = "md5"
	sysExcludeTag   = "sys-v1"

	// spillThreshold is §10.3 / --spill-threshold. For the vertical slice the
	// external merge sort is a documented follow-up (B-MEM-01); above the
	// threshold we still sort in memory.
	spillThreshold = 500_000
)

// Exclusion records one entry removed before the plan (§10.1.1, §10.1). Prune is
// true for a directory the walker must not descend into.
type Exclusion struct {
	Path  []byte
	Rule  string
	Prune bool
}

// Plan is the ordered, grouped, digested result of Build.
type Plan struct {
	entries       []Entry
	keys          [][]byte
	idByKey       map[string]uint64
	exclusions    []Exclusion
	filterSig     string
	digest        [32]byte
	TotalFiles    uint64
	TotalBytes    uint64
	ExcludedCount uint64
}

// Build applies the system-file exclusion set and the user filters, sorts the
// survivors by the §10.2 ordering key, groups them, and computes the manifest
// digest (§10.5). The input slice is not retained.
func Build(entries []Entry, opts Options) (*Plan, error) {
	if err := validateFilters(opts.Filters); err != nil {
		return nil, err
	}

	p := &Plan{filterSig: opts.FilterSignature()}

	type keyed struct {
		e    Entry
		key  []byte
		orig []byte
	}
	ks := make([]keyed, 0, len(entries))

	for _, e := range entries {
		slash := toSlash(e.RelPath)

		if !opts.KeepSystemFiles {
			if rule, prune, ok := MatchSystemFile(finalComponent(slash), e.Type == TypeDir, atRoot(slash)); ok {
				p.exclusions = append(p.exclusions, Exclusion{
					Path:  clone(e.RelPath),
					Rule:  rule,
					Prune: prune,
				})
				p.ExcludedCount++
				continue
			}
		}

		if admit, rule := opts.filterAdmits(string(slash), e.Type == TypeDir); !admit {
			p.exclusions = append(p.exclusions, Exclusion{
				Path:  clone(e.RelPath),
				Rule:  "filter:" + rule,
				Prune: e.Type == TypeDir,
			})
			p.ExcludedCount++
			continue
		}

		key, nonUTF8 := orderingKey(slash)
		if nonUTF8 {
			e.Flags |= FlagNameNonUTF8
		}
		ks = append(ks, keyed{e: e, key: key, orig: slash})
	}

	if len(ks) > spillThreshold {
		// TODO(B-MEM-01): external run/merge sort. The slice sorts in memory.
	}
	slices.SortFunc(ks, func(a, b keyed) int {
		if c := bytes.Compare(a.key, b.key); c != 0 {
			return c
		}
		return bytes.Compare(a.orig, b.orig)
	})

	p.entries = make([]Entry, len(ks))
	p.keys = make([][]byte, len(ks))
	p.idByKey = make(map[string]uint64, len(ks))
	for i, k := range ks {
		p.entries[i] = k.e
		p.keys[i] = k.key
		if _, dup := p.idByKey[string(k.key)]; !dup {
			p.idByKey[string(k.key)] = uint64(i)
		}
		if k.e.Type == TypeFile {
			p.TotalFiles++
			p.TotalBytes += uint64(k.e.Size)
		}
	}
	p.digest = computeDigest(p.entries, p.keys, p.filterSig)
	return p, nil
}

// orderingKey is key(entry) from §10.2: the '/'-separated path, NFC-normalised
// when it is valid UTF-8, as raw unsigned bytes. Invalid UTF-8 names are not
// normalised and the second return is true.
func orderingKey(slash []byte) (key []byte, nonUTF8 bool) {
	if !utf8.Valid(slash) {
		return slash, true
	}
	return norm.NFC.Bytes(slash), false
}

// toSlash replaces the OS path separator with '/'. On a '/'-separator OS the
// input is returned unchanged.
func toSlash(p []byte) []byte {
	if os.PathSeparator == '/' {
		return p
	}
	out := make([]byte, len(p))
	for i, c := range p {
		if c == os.PathSeparator {
			out[i] = '/'
		} else {
			out[i] = c
		}
	}
	return out
}

func finalComponent(slash []byte) []byte {
	if i := bytes.LastIndexByte(slash, '/'); i >= 0 {
		return slash[i+1:]
	}
	return slash
}

func atRoot(slash []byte) bool { return bytes.IndexByte(slash, '/') < 0 }

func clone(b []byte) []byte { return append([]byte(nil), b...) }

func computeDigest(entries []Entry, keys [][]byte, filterSig string) [32]byte {
	h := sha256.New()
	var n [8]byte

	h.Write([]byte("esync/v1 manifest"))
	binary.BigEndian.PutUint16(n[:2], protocolVersion)
	h.Write(n[:2])
	h.Write([]byte(hashAlg))
	binary.BigEndian.PutUint32(n[:4], groupSize)
	h.Write(n[:4])
	binary.BigEndian.PutUint32(n[:4], 0) // reserved plan flags
	h.Write(n[:4])
	h.Write([]byte(filterSig))
	h.Write([]byte(sysExcludeTag))

	for i, e := range entries {
		k := keys[i]
		binary.BigEndian.PutUint32(n[:4], uint32(len(k)))
		h.Write(n[:4])
		h.Write(k)
		h.Write([]byte{byte(e.Type)})
		binary.BigEndian.PutUint64(n[:8], uint64(e.Size))
		h.Write(n[:8])
		binary.BigEndian.PutUint64(n[:8], uint64(e.MtimeSec))
		h.Write(n[:8])
		binary.BigEndian.PutUint32(n[:4], e.MtimeNsec)
		h.Write(n[:4])
	}

	var out [32]byte
	h.Sum(out[:0])
	return out
}

// Entries returns the plan entries in sorted order.
func (p *Plan) Entries() []Entry { return p.entries }

// NumGroups is the number of groups; an empty plan has zero (§10.4).
func (p *Plan) NumGroups() int { return (len(p.entries) + groupSize - 1) / groupSize }

// Group returns the entries of group i (a contiguous file-id range).
func (p *Plan) Group(i int) []Entry {
	lo := i * groupSize
	hi := lo + groupSize
	if hi > len(p.entries) {
		hi = len(p.entries)
	}
	return p.entries[lo:hi]
}

// Groups returns every group in order.
func (p *Plan) Groups() [][]Entry {
	gs := make([][]Entry, p.NumGroups())
	for i := range gs {
		gs[i] = p.Group(i)
	}
	return gs
}

// FileID maps a source-relative '/'-path to its file id. The lookup normalises
// the argument with the §10.2 key rule.
func (p *Plan) FileID(relSlashPath string) (uint64, bool) {
	key, _ := orderingKey([]byte(relSlashPath))
	id, ok := p.idByKey[string(key)]
	return id, ok
}

// GroupID returns file_id / 1024 (§10.4).
func GroupID(fileID uint64) uint64 { return fileID / groupSize }

// ManifestDigest is the §10.5 fingerprint of the plan (not of content).
func (p *Plan) ManifestDigest() [32]byte { return p.digest }

// Exclusions lists every entry dropped or pruned by the sys set or the filters.
func (p *Plan) Exclusions() []Exclusion { return p.exclusions }

// FilterSignature is the canonical filter/sys-set string folded into the digest.
func (p *Plan) FilterSignature() string { return p.filterSig }

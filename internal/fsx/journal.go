package fsx

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"esync/internal/fault"
)

// Record is one completed-file entry in complete.log (§12.6).
type Record struct {
	FileID uint64
	Path   string
	Digest []byte
	Size   int64
}

type sessionInfo struct {
	SessionID       string `json:"session_id"`
	ManifestDigest  string `json:"manifest_digest"`
	SourceRootName  string `json:"source_root_name"`
	ProtocolVersion int    `json:"protocol_version"`
	StartedAt       string `json:"started_at"`
}

// Journal is the resume journal under <dest>/.esync/ (§12.6).
type Journal struct {
	root *os.Root
	mu   sync.Mutex
	log  *os.File

	// Resumed is true when a prior session with a matching manifest digest was
	// found and its completed files were returned by Open.
	Resumed bool
}

const journalDir = ".esync"

// Open prepares the journal under dest. If a prior session.json is present and
// its manifest digest matches, journalled files are returned in priorComplete
// and Resumed is set; a mismatch discards the prior .esync/ directory.
func Open(dest, sessionID string, manifestDigest [32]byte) (j *Journal, priorComplete map[uint64]Record, err error) {
	root, oerr := os.OpenRoot(dest)
	if oerr != nil {
		return nil, nil, fault.New(fault.E1006, "open destination root", dest, oerr)
	}
	defer func() {
		if err != nil {
			_ = root.Close()
		}
	}()

	digestHex := hex.EncodeToString(manifestDigest[:])
	priorComplete = make(map[uint64]Record)
	resumed := false

	if e := root.Mkdir(journalDir, 0o700); e != nil && !errors.Is(e, os.ErrExist) {
		return nil, nil, fault.New(diskCode(e, fault.E7007), "create journal directory", journalDir, e)
	}

	if data, e := root.ReadFile(journalDir + "/session.json"); e == nil {
		var si sessionInfo
		if json.Unmarshal(data, &si) == nil && si.ManifestDigest == digestHex {
			resumed = true
			if lg, e2 := root.ReadFile(journalDir + "/complete.log"); e2 == nil {
				priorComplete = parseCompleteLog(lg)
			}
		} else {
			_ = root.RemoveAll(journalDir)
			if e := root.Mkdir(journalDir, 0o700); e != nil {
				return nil, nil, fault.New(diskCode(e, fault.E7007), "recreate journal directory", journalDir, e)
			}
		}
	}

	if !resumed {
		si := sessionInfo{
			SessionID:       sessionID,
			ManifestDigest:  digestHex,
			SourceRootName:  "",
			ProtocolVersion: 2,
			StartedAt:       time.Now().UTC().Format(time.RFC3339),
		}
		buf, _ := json.MarshalIndent(si, "", "  ")
		if e := root.WriteFile(journalDir+"/session.json", buf, 0o600); e != nil {
			return nil, nil, fault.New(diskCode(e, fault.E7007), "write session.json", "session.json", e)
		}
	}

	if e := root.Mkdir(journalDir+"/parts", 0o700); e != nil && !errors.Is(e, os.ErrExist) {
		return nil, nil, fault.New(diskCode(e, fault.E7007), "create parts directory", "parts", e)
	}

	lf, e := root.OpenFile(journalDir+"/complete.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if e != nil {
		return nil, nil, fault.New(diskCode(e, fault.E7007), "open complete.log", "complete.log", e)
	}

	return &Journal{root: root, log: lf, Resumed: resumed}, priorComplete, nil
}

// MarkComplete appends rec to complete.log (buffered by the OS; Sync flushes).
func (j *Journal) MarkComplete(rec Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.log == nil {
		return fault.New(fault.E7007, "journal write", "complete.log", errors.New("journal closed"))
	}
	if _, err := j.log.WriteString(encodeRecord(rec)); err != nil {
		return fault.New(fault.E7007, "journal write", "complete.log", err)
	}
	return nil
}

// Sync fsyncs complete.log; called at group boundaries.
func (j *Journal) Sync() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.log == nil {
		return nil
	}
	if err := j.log.Sync(); err != nil {
		return fault.New(fault.E7007, "journal sync", "complete.log", err)
	}
	return nil
}

// Discard removes the whole .esync/ directory (manifest digest mismatch).
func (j *Journal) Discard() error { return j.remove() }

// Clear removes the whole .esync/ directory on successful completion.
func (j *Journal) Clear() error { return j.remove() }

func (j *Journal) remove() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.log != nil {
		_ = j.log.Close()
		j.log = nil
	}
	if err := j.root.RemoveAll(journalDir); err != nil {
		return fault.New(fault.E7007, "remove journal directory", journalDir, err)
	}
	return nil
}

// Close closes the log file without removing the journal (retained for resume).
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.log != nil {
		err := j.log.Close()
		j.log = nil
		if err != nil {
			return fault.New(fault.E7007, "close complete.log", "complete.log", err)
		}
	}
	_ = j.root.Close()
	return nil
}

func encodeRecord(r Record) string {
	body := strconv.FormatUint(r.FileID, 10) + " " +
		strconv.FormatInt(r.Size, 10) + " " +
		hex.EncodeToString(r.Digest) + " " +
		base64.StdEncoding.EncodeToString([]byte(r.Path))
	crc := crc32.ChecksumIEEE([]byte(body))
	return body + " " + strconv.FormatUint(uint64(crc), 16) + "\n"
}

// parseCompleteLog decodes complete.log, stopping at the first corrupt record so
// a torn final write is discarded (§12.6, E8004-warn).
func parseCompleteLog(data []byte) map[uint64]Record {
	out := make(map[uint64]Record)
	for _, ln := range bytes.Split(data, []byte{'\n'}) {
		if len(ln) == 0 {
			continue
		}
		rec, ok := decodeRecord(string(ln))
		if !ok {
			break
		}
		out[rec.FileID] = rec
	}
	return out
}

func decodeRecord(line string) (Record, bool) {
	f := strings.Fields(line)
	if len(f) != 5 {
		return Record{}, false
	}
	body := f[0] + " " + f[1] + " " + f[2] + " " + f[3]
	crc, err := strconv.ParseUint(f[4], 16, 32)
	if err != nil || uint32(crc) != crc32.ChecksumIEEE([]byte(body)) {
		return Record{}, false
	}
	fid, e1 := strconv.ParseUint(f[0], 10, 64)
	size, e2 := strconv.ParseInt(f[1], 10, 64)
	dig, e3 := hex.DecodeString(f[2])
	pth, e4 := base64.StdEncoding.DecodeString(f[3])
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
		return Record{}, false
	}
	return Record{FileID: fid, Size: size, Digest: dig, Path: string(pth)}, true
}

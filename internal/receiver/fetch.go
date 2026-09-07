package receiver

import (
	"bytes"
	"context"
	"errors"
	"hash"
	"io"
	"math/rand"
	"os"
	"strconv"
	"syscall"
	"time"

	"esync/internal/channel"
	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/fsx"
	"esync/internal/obs"
	"esync/internal/wire"
)

// fetcher pulls needed files off the queue on one data channel, requests them,
// verifies them, and publishes them atomically (ARCHITECTURE §12.2). The
// effective request depth is one per channel in this build (see follow-ups).
type fetcher struct {
	octx     obs.Ctx
	cfg      Config
	conn     *channel.Conn
	dest     *fsx.Dest
	journal  *fsx.Journal
	algo     digest.Algo
	q        *needQueue
	meta     *metaStore
	hl       *hardlinkMap
	counters *obs.Counters
	reqID    interface{ Add(uint64) uint64 }
	rnd      *rand.Rand
	stall    time.Duration // per-receive ceiling on a data channel; 0 disables

	onComplete func(fileID uint64)
	fail       func(error)
}

func (f *fetcher) loop(ctx context.Context) {
	for {
		it, ok := f.q.pop(ctx)
		if !ok {
			return
		}
		err := f.fetchOne(ctx, it)
		if err == nil {
			f.q.done(it.groupID)
			continue
		}
		if ctx.Err() != nil {
			f.q.done(it.groupID)
			return
		}
		if fault.IsFatal(err) {
			f.fail(err)
			f.q.done(it.groupID)
			return
		}
		if fault.IsRetryable(err) && it.attempt+1 <= f.cfg.MaxRetries {
			f.counters.Retries.Add(1)
			obs.Warn(f.octx, "file transfer failed, retrying",
				obs.F("file", it.fileID), obs.F("attempt", it.attempt+1), obs.F("err", err.Error()))
			select {
			case <-time.After(fault.Backoff(it.attempt, f.rnd)):
			case <-ctx.Done():
				f.q.done(it.groupID)
				return
			}
			it.attempt++
			f.q.requeue(it) // re-enters the queue; may be served by another channel
			continue
		}
		obs.LogFault(f.octx, err)
		f.counters.FilesFailed.Add(1)
		f.q.done(it.groupID)
	}
}

func (f *fetcher) fetchOne(ctx context.Context, it needItem) error {
	fm, ok := f.meta.get(it.fileID)
	if !ok {
		return fault.Newf(fault.E5001, "fetch file", strconv.FormatUint(it.fileID, 10), nil, "no manifest metadata")
	}

	reqID := f.reqID.Add(1)
	if err := f.conn.SendMsg(&wire.FileRequest{
		RequestID: reqID, FileID: it.fileID, Offset: 0, MaxChunk: safeMaxChunk,
	}); err != nil {
		return transportData(err)
	}

	m, err := f.conn.RecvMsgTimeout(f.stall)
	if err != nil {
		return transportData(err)
	}
	var hdr *wire.FileHeader
	switch v := m.(type) {
	case *wire.FileHeader:
		hdr = v
	case *wire.FileError:
		return faultFromFileError(v)
	default:
		return fault.Newf(fault.E5001, "await FILE_HEADER", m.Type().String(), nil, "unexpected message")
	}

	part, err := f.dest.OpenPart(it.fileID)
	if err != nil {
		return err
	}
	if err := part.Truncate(0); err != nil {
		part.Close()
		return fault.New(fault.E7004, "truncate part file", fm.rel, err)
	}
	if _, err := part.Seek(0, io.SeekStart); err != nil {
		part.Close()
		return fault.New(fault.E7004, "seek part file", fm.rel, err)
	}

	h, herr := digest.New(f.algo)
	if herr != nil {
		part.Close()
		return herr
	}

	var written uint64
	for {
		m, err := f.conn.RecvMsgTimeout(f.stall)
		if err != nil {
			part.Close()
			return transportData(err)
		}
		switch v := m.(type) {
		case *wire.FileChunk:
			if v.Offset != written {
				part.Close()
				return fault.Newf(fault.E5006, "verify chunk offset", fm.rel, nil,
					"got offset %d, want %d", v.Offset, written)
			}
			if _, werr := part.Write(v.Data); werr != nil {
				part.Close()
				code := fault.E7004
				if errors.Is(werr, syscall.ENOSPC) {
					code = fault.E7003
				}
				return fault.New(code, "write part file", fm.rel, werr)
			}
			h.Write(v.Data)
			written += uint64(len(v.Data))
			f.counters.Bytes.Add(int64(len(v.Data)))
		case *wire.FileComplete:
			if cerr := part.Close(); cerr != nil {
				return fault.New(fault.E7004, "close part file", fm.rel, cerr)
			}
			return f.finish(it, fm, hdr, v, h, written)
		case *wire.FileError:
			part.Close()
			return faultFromFileError(v)
		default:
			part.Close()
			return fault.Newf(fault.E5001, "receive file content", m.Type().String(), nil, "unexpected message")
		}
	}
}

func (f *fetcher) finish(it needItem, fm fileMeta, hdr *wire.FileHeader, fc *wire.FileComplete, h hash.Hash, written uint64) error {
	local := h.Sum(nil)

	if written != hdr.Size {
		return fault.Newf(fault.E8002, "verify byte count", fm.rel, nil,
			"wrote %d bytes, header declared %d", written, hdr.Size)
	}
	if len(fc.DigestFull) > 0 && !bytes.Equal(local, fc.DigestFull) {
		return fault.New(fault.E8001, "verify streamed digest", fm.rel, nil)
	}
	if len(fm.digest) > 0 && !bytes.Equal(local, fm.digest) {
		return fault.New(fault.E8003, "verify content against manifest", fm.rel, nil)
	}
	if len(hdr.Digest) > 0 && !bytes.Equal(local, hdr.Digest) {
		return fault.New(fault.E8003, "verify content against manifest", fm.rel, nil)
	}

	mode := fm.mode
	if mode == 0 {
		mode = os.FileMode(hdr.Mode).Perm()
	}
	if err := f.dest.PublishPart(it.fileID, fm.rel, mode, fm.mtime); err != nil {
		if fault.GetCode(err) == fault.E7006 {
			obs.LogFault(f.octx, err) // content is correct; metadata is not
		} else {
			return err
		}
	}

	if f.journal != nil {
		if err := f.journal.MarkComplete(fsx.Record{
			FileID: it.fileID, Path: fm.rel, Digest: local, Size: int64(written),
		}); err != nil {
			return err // E7007, fatal
		}
	}
	if fm.hasHLKey {
		for _, sec := range f.hl.registerMaterialised(it.fileID, fm.rel) {
			if err := f.dest.MakeHardlink(sec, fm.rel); err != nil {
				obs.LogFault(f.octx, err) // §12.4: a failed link is a warning, not fatal
			}
		}
	}

	f.counters.FilesTransferred.Add(1)
	if it.attempt > 0 {
		f.counters.RetriesOK.Add(1)
	}
	f.onComplete(it.fileID)
	obs.Trace(f.octx, "file published", obs.F("file", it.fileID), obs.F("path", fm.rel), obs.F("bytes", written))
	return nil
}

// faultFromFileError reconstructs a Fault from a FILE_ERROR. Class and
// retryability are taken from the code catalogue (§14.1), never from the
// sender's hint.
func faultFromFileError(fe *wire.FileError) error {
	code := fault.Code("E" + pad4(fe.Code))
	msg := fe.Message
	if msg == "" {
		msg = "sender reported a file error"
	}
	return fault.Newf(code, "sender reported a file error", "", nil, "%s", msg)
}

func pad4(n uint16) string {
	s := strconv.FormatUint(uint64(n), 10)
	for len(s) < 4 {
		s = "0" + s
	}
	return s
}

func transportData(err error) error {
	if err == nil {
		return nil
	}
	if fault.GetCode(err) != "" {
		return err
	}
	if errors.Is(err, io.EOF) {
		return fault.New(fault.E3005, "data channel closed", "", err)
	}
	return fault.Wrap(fault.E3005, "data channel transport", "", err)
}

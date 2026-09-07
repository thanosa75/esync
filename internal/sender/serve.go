package sender

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"esync/internal/channel"
	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/obs"
	"esync/internal/plan"
	"esync/internal/wire"
)

// servicer answers FILE_REQUESTs on one data channel (ARCHITECTURE §12.1). One
// runs per data channel; they share the read-concurrency semaphore.
type servicer struct {
	ctx       obs.Ctx
	conn      *channel.Conn
	entries   []plan.Entry
	root      string
	algo      digest.Algo
	chunkSize int
	readSem   chan struct{}
	digests   *digestStore
	tr        *tracker
}

// run reads requests until the peer closes the channel (io.EOF) or the context
// is cancelled. A transport error is returned to the caller for channel-loss
// handling.
func (s *servicer) run(ctx context.Context) error {
	for {
		msg, err := s.conn.RecvMsg()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		switch m := msg.(type) {
		case *wire.FileRequest:
			s.tr.reqStarted(m.RequestID, m.FileID)
			if err := s.serve(ctx, m); err != nil {
				return err
			}
		case *wire.FileCancel:
			s.tr.reqCancelled(m.RequestID)
		default:
			return fault.Newf(fault.E5001, "serve", msg.Type().String(), nil,
				"unexpected message on data channel")
		}
	}
}

func (s *servicer) serve(ctx context.Context, req *wire.FileRequest) error {
	end := obs.Start(s.ctx, "file.transfer")

	if req.FileID >= uint64(len(s.entries)) {
		_ = s.sendErr(req, fault.E5007, false, "unknown file_id")
		end("error")
		return fault.New(fault.E5007, "serve file request", strconv.FormatUint(req.FileID, 10), nil)
	}
	e := s.entries[req.FileID]
	if e.Type != plan.TypeFile {
		_ = s.sendErr(req, fault.E5007, false, "file_id is not a regular file")
		s.tr.reqFailed(req.RequestID, true, true)
		end("error")
		return nil
	}

	abs := filepath.Join(s.root, string(e.RelPath))

	select {
	case s.readSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.readSem }()

	f, err := os.Open(abs)
	if err != nil {
		code := mapOpenErr(err)
		retry := code == fault.E6004
		_ = s.sendErr(req, code, retry, err.Error())
		s.tr.reqFailed(req.RequestID, !retry, !retry)
		end("error", obs.F("code", string(code)))
		return nil
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		_ = s.sendErr(req, fault.E6004, true, err.Error())
		s.tr.reqFailed(req.RequestID, false, false)
		end("error")
		return nil
	}
	if uint64(fi.Size()) != uint64(e.Size) {
		_ = s.sendErr(req, fault.E6003, false, "source size changed since scan")
		s.tr.reqFailed(req.RequestID, true, true)
		end("error", obs.F("code", string(fault.E6003)))
		return nil
	}

	manifestDigest, _ := s.digests.get(req.FileID)
	if err := s.conn.SendMsg(&wire.FileHeader{
		RequestID: req.RequestID,
		FileID:    req.FileID,
		Size:      uint64(e.Size),
		Mode:      uint32(e.Mode.Perm()),
		MtimeSec:  e.MtimeSec,
		MtimeNsec: e.MtimeNsec,
		Digest:    manifestDigest,
		ChunkSize: uint32(s.chunkSize),
	}); err != nil {
		return err
	}

	off := int64(req.Offset)
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			_ = s.sendErr(req, fault.E6004, true, err.Error())
			s.tr.reqFailed(req.RequestID, false, false)
			end("error")
			return nil
		}
	}

	h, err := digest.New(s.algo)
	if err != nil {
		return fault.Wrap(fault.E9002, "serve", abs, err)
	}
	buf := make([]byte, s.chunkSize)
	var sent uint64
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			if err := s.conn.SendMsg(&wire.FileChunk{
				RequestID: req.RequestID,
				Offset:    uint64(off) + sent,
				Data:      buf[:n],
			}); err != nil {
				return err
			}
			sent += uint64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = s.sendErr(req, fault.E6004, true, rerr.Error())
			s.tr.reqFailed(req.RequestID, false, false)
			end("error")
			return nil
		}
	}

	if off+int64(sent) != e.Size {
		_ = s.sendErr(req, fault.E6003, false, "short read: source shrank during transfer")
		s.tr.reqFailed(req.RequestID, true, true)
		end("error", obs.F("code", string(fault.E6003)))
		return nil
	}

	full := h.Sum(nil)
	if off == 0 && len(manifestDigest) > 0 && !bytes.Equal(full, manifestDigest) {
		_ = s.sendErr(req, fault.E8003, false, "streamed digest does not match the manifest")
		s.tr.reqFailed(req.RequestID, true, true)
		end("error", obs.F("code", string(fault.E8003)))
		return nil
	}

	if err := s.conn.SendMsg(&wire.FileComplete{
		RequestID:  req.RequestID,
		BytesSent:  sent,
		DigestFull: full,
	}); err != nil {
		return err
	}
	s.tr.reqCompleted(req.RequestID, req.FileID, sent)
	s.ctx.Counters().FilesTransferred.Add(1)
	s.ctx.Counters().Bytes.Add(int64(sent))
	end("ok", obs.F("bytes", sent))
	return nil
}

func (s *servicer) sendErr(req *wire.FileRequest, code fault.Code, retryable bool, msg string) error {
	var r uint8
	if retryable {
		r = 1
	}
	return s.conn.SendMsg(&wire.FileError{
		RequestID: req.RequestID,
		Code:      codeNum(code),
		Retryable: r,
		Message:   msg,
	})
}

func mapOpenErr(err error) fault.Code {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fault.E6002
	case errors.Is(err, fs.ErrPermission):
		return fault.E6001
	default:
		return fault.E6004
	}
}

func codeNum(c fault.Code) uint16 {
	n, _ := strconv.Atoi(strings.TrimPrefix(string(c), "E"))
	return uint16(n)
}

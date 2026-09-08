package receiver

// Item-2 / REQ-NET-008 (ARCHITECTURE §7.3 row 1): a data channel that closes or
// errors mid-file is recoverable. The in-flight file returns to the need queue
// and the worker rejoins with a fresh CHANNEL_JOIN (0.5/2/8 s backoff); the
// session continues. Only when the last channel is retired with work remaining
// is it fatal E3005. These tests drive a real fetcher/session against a fake
// sender over real loopback TCP and a real handshake session.

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"esync/internal/channel"
	"esync/internal/crypto/handshake"
	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/fsx"
	"esync/internal/obs"
	"esync/internal/wire"
)

// pairSessions runs one real pair handshake and returns the two sessions
// (receiver side and sender side) plus the sender-side listener address that
// data channels join through.
func pairSessions(t *testing.T) (client, server *handshake.Session) {
	t.Helper()
	cli, srv := tcpPair(t)
	type r struct {
		s   *handshake.Session
		err error
	}
	cch := make(chan r, 1)
	go func() {
		s, err := handshake.ClientHandshake(obs.Ctx{}, cli, testKPair, testSID, 3*time.Second)
		cch <- r{s, err}
	}()
	ssrv, err := handshake.ServerHandshake(obs.Ctx{}, srv, testKPair, testSID, 3*time.Second)
	if err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	cr := <-cch
	if cr.err != nil {
		t.Fatalf("client handshake: %v", cr.err)
	}
	return cr.s, ssrv
}

// fakeDataSender accepts data-channel joins on a listener and serves the given
// file map on every connection, except that the first join (when killOnce is
// set) answers one request with a header + partial chunk and then closes the
// connection mid-file.
type fakeDataSender struct {
	ln       net.Listener
	sess     *handshake.Session
	bound    uint8
	files    map[uint64]*stubFile
	killNext atomic.Bool
}

func startFakeDataSender(t *testing.T, sess *handshake.Session, files map[uint64]*stubFile, killFirst, closeAfterKill bool) (addr string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDataSender{ln: ln, sess: sess, bound: 8, files: files}
	f.killNext.Store(killFirst)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if f.killNext.CompareAndSwap(true, false) {
				if closeAfterKill {
					ln.Close() // sender gone: later rejoins are refused
				}
				go f.serveAndDie(c)
			} else {
				go serveJoined(t, f.sess, f.bound, c, f.files)
			}
		}
	}()
	return ln.Addr().String()
}

// serveAndDie joins one channel, answers the first FILE_REQUEST with a header
// and a partial chunk, then closes the connection — the receiver must see this
// as recoverable channel loss, requeue the file, and rejoin.
func (f *fakeDataSender) serveAndDie(c net.Conn) {
	id, err := handshake.AcceptChannel(obs.Ctx{}, c, f.sess, f.bound)
	if err != nil {
		c.Close()
		return
	}
	conn := channel.Data(c, id, f.sess, channel.Sender, 0)
	defer conn.Close()
	m, err := conn.RecvMsg()
	if err != nil {
		return
	}
	req, ok := m.(*wire.FileRequest)
	if !ok {
		return
	}
	sf := f.files[req.FileID]
	if sf == nil {
		return
	}
	_ = conn.SendMsg(&wire.FileHeader{
		RequestID: req.RequestID, FileID: req.FileID,
		Size: uint64(len(sf.data)), Mode: 0o644, Digest: sf.dig, ChunkSize: 1 << 16,
	})
	const cs = 1 << 16
	if len(sf.data) > cs {
		_ = conn.SendMsg(&wire.FileChunk{RequestID: req.RequestID, Offset: 0, Data: sf.data[:cs]})
	}
	// authenticated close, mid-file
}

// serveJoined is serveRequests (fetch_test.go) for a freshly joined connection.
func serveJoined(t *testing.T, sess *handshake.Session, bound uint8, c net.Conn, files map[uint64]*stubFile) {
	t.Helper()
	id, err := handshake.AcceptChannel(obs.Ctx{}, c, sess, bound)
	if err != nil {
		c.Close()
		return
	}
	serveRequests(t, channel.Data(c, id, sess, channel.Sender, 0), files)
}

// rejoinSession builds a session wired to addr with a need queue, journal and
// manifest store, ready for addChannel/registerAndStart to run against the fake
// sender.
func rejoinSession(t *testing.T, addr string, hsess *handshake.Session) *session {
	t.Helper()
	dir := t.TempDir()
	dest, err := fsx.OpenDest(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dest.Close() })
	j, _, err := fsx.Open(dir, "0000000000000000", [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })

	rootCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	s := &session{
		cfg:               Config{}.withDefaults(),
		rootCtx:           rootCtx,
		cancel:            cancel,
		destPath:          dir,
		cnt:               &obs.Counters{},
		senderMaxChannels: 8,
		addr:              addr,
		hsess:             hsess,
		dest:              dest,
		journal:           j,
		algo:              digest.MD5,
		q:                 newNeedQueue(needQueueDepth),
		meta:              newMetaStore(),
		hl:                newHardlinkMap(),
	}
	return s
}

func (s *session) enqueueTwo(t *testing.T, data0, data1 []byte) {
	t.Helper()
	dig0, dig1 := md5sum(t, data0), md5sum(t, data1)
	s.meta.set(0, fileMeta{rel: "a.bin", size: int64(len(data0)), mode: 0o644, mtime: time.Unix(1000, 0), digest: dig0})
	s.meta.set(1, fileMeta{rel: "b.bin", size: int64(len(data1)), mode: 0o644, mtime: time.Unix(1000, 0), digest: dig1})
	if err := s.q.pushGroup(context.Background(), []needItem{
		{fileID: 0, groupID: 0, size: int64(len(data0))},
		{fileID: 1, groupID: 0, size: int64(len(data1))},
	}); err != nil {
		t.Fatal(err)
	}
	s.q.close()
}

func waitForFiles(t *testing.T, dir string, want ...string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		all := true
		for _, rel := range want {
			if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(rel))); err != nil {
				all = false
				break
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %v under %s", want, dir)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// F-NET-01: killing a data channel mid-file does not fail the session. The file
// is requeued, a fresh CHANNEL_JOIN brings a replacement channel up, and both
// files are published; exit stays clean.
func TestChannelLossRecoversByRejoin(t *testing.T) {
	hsess, ssess := pairSessions(t)
	data0 := make([]byte, 3<<16) // several chunks, killed part-way through
	data1 := []byte("second file")
	addr := startFakeDataSender(t, ssess, map[uint64]*stubFile{
		0: {data: data0, dig: md5sum(t, data0)},
		1: {data: data1, dig: md5sum(t, data1)},
	}, true, false)

	s := rejoinSession(t, addr, hsess)
	s.enqueueTwo(t, data0, data1)
	if err := s.addChannel(obs.Ctx{}); err != nil {
		t.Fatalf("first channel: %v", err)
	}

	waitForFiles(t, s.destPath, "a.bin", "b.bin")
	if s.err() != nil {
		t.Fatalf("session failed after a recoverable channel loss: %v", s.err())
	}
	if s.cnt.Retries.Load() < 1 {
		t.Fatalf("channel loss was not counted as a retry (Retries=%d)", s.cnt.Retries.Load())
	}
	if s.cnt.FilesFailed.Load() != 0 || s.cnt.FilesTransferred.Load() != 2 {
		t.Fatalf("failed=%d transferred=%d, want 0/2", s.cnt.FilesFailed.Load(), s.cnt.FilesTransferred.Load())
	}
	got0, _ := os.ReadFile(filepath.Join(s.destPath, "a.bin"))
	got1, _ := os.ReadFile(filepath.Join(s.destPath, "b.bin"))
	if string(got0) != string(data0) || string(got1) != string(data1) {
		t.Fatal("published content differs")
	}
}

// A channel that dies while the worker is idle between requests (nothing in
// flight) also recovers: the next file simply goes out over the rejoined
// channel.
func TestChannelLossIdleThenRejoin(t *testing.T) {
	hsess, ssess := pairSessions(t)
	data := []byte("payload after an idle loss")
	addr := startFakeDataSender(t, ssess, map[uint64]*stubFile{
		0: {data: data, dig: md5sum(t, data)},
	}, true, false) // the first join dies as soon as it answers a request

	s := rejoinSession(t, addr, hsess)
	s.meta.set(0, fileMeta{rel: "idle.bin", size: int64(len(data)), mode: 0o644, mtime: time.Unix(1000, 0), digest: md5sum(t, data)})
	if err := s.q.pushGroup(context.Background(), []needItem{{fileID: 0, size: int64(len(data))}}); err != nil {
		t.Fatal(err)
	}
	s.q.close()
	if err := s.addChannel(obs.Ctx{}); err != nil {
		t.Fatalf("first channel: %v", err)
	}

	waitForFiles(t, s.destPath, "idle.bin")
	if s.err() != nil {
		t.Fatalf("session failed: %v", s.err())
	}
}

// §7.3: when every rejoin attempt fails, the channel is retired, and retiring
// the last channel with work still queued is fatal E3005.
func TestChannelRejoinExhaustedRetiresToE3005(t *testing.T) {
	hsess, ssess := pairSessions(t)
	data := make([]byte, 2<<16)
	addr := startFakeDataSender(t, ssess, map[uint64]*stubFile{0: {data: data, dig: md5sum(t, data)}}, true, true)

	s := rejoinSession(t, addr, hsess)
	s.enqueueTwo(t, data, []byte("never fetched"))

	// Stop accepting: the sender vanishes after the first (killed) connection.
	save := joinBackoff
	joinBackoff = [...]time.Duration{20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond}
	t.Cleanup(func() { joinBackoff = save })

	if err := s.addChannel(obs.Ctx{}); err != nil {
		t.Fatalf("first channel: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for s.err() == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if s.err() == nil {
		t.Fatal("session did not fail after the last channel was retired with work queued")
	}
	if code := faultCode(t, s.err()); code != "E3005" {
		t.Fatalf("fatal error = %v, want E3005", s.err())
	}
}

func faultCode(t *testing.T, err error) string {
	t.Helper()
	c := fault.GetCode(err)
	if c == "" {
		t.Fatalf("error %v carries no code", err)
	}
	return string(c)
}

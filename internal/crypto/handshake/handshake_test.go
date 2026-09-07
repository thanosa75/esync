package handshake

import (
	"bytes"
	"encoding/hex"
	"io"
	"net"
	"testing"
	"time"

	"esync/internal/fault"
	"esync/internal/obs"
)

func fixed(n int, b byte) []byte { return bytes.Repeat([]byte{b}, n) }

// G-KDF-01 / REQ-SEC-020: a fixed (nonce_c, nonce_s, Z, K_pair) derives exactly
// these keys. Locks the §6.3 key schedule (labels, order, HKDF construction).
func TestGoldenKeySchedule(t *testing.T) {
	sch, err := newSchedule(fixed(32, 0xc1), fixed(32, 0x53), fixed(32, 0x5a), fixed(32, 0x9d))
	if err != nil {
		t.Fatal(err)
	}
	e0s, m0s, _ := sch.recordKeys(S2R, 0)
	e1r, m1r, _ := sch.recordKeys(R2S, 1)
	got := map[string]string{
		"K_conf":      hex.EncodeToString(sch.kConf.Bytes()),
		"K_chan":      hex.EncodeToString(sch.kChan.Bytes()),
		"K_enc/s2r/0": hex.EncodeToString(e0s.Bytes()),
		"K_mac/s2r/0": hex.EncodeToString(m0s.Bytes()),
		"K_enc/r2s/1": hex.EncodeToString(e1r.Bytes()),
		"K_mac/r2s/1": hex.EncodeToString(m1r.Bytes()),
	}
	want := map[string]string{
		"K_conf":      goldenKConf,
		"K_chan":      goldenKChan,
		"K_enc/s2r/0": goldenEnc0S,
		"K_mac/s2r/0": goldenMac0S,
		"K_enc/r2s/1": goldenEnc1R,
		"K_mac/r2s/1": goldenMac1R,
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s = %s, want %s", k, got[k], w)
		}
	}
}

const (
	goldenKConf = "f12b6b85c011d5bae0c40960ef613e68fe6fc94d0403d2057c2bd3ee9c04ae41"
	goldenKChan = "20c35955222c02fe608b3e696ebb15e7f49087463450493ec16236100adb2835"
	goldenEnc0S = "6e3491934bb6745e6da0f75cd6190d0eb4ecfd694815e47b3d35f349b0a88ee0"
	goldenMac0S = "7ecd02d1952b47a13ecd9ab5c68d6759a88b97655eb409d2b32cc459b462f854"
	goldenEnc1R = "57a97f5bb6757830b529f6fbda6bd993dc83e5b1962c64dc4e166e3fa03fa93d"
	goldenMac1R = "2879b3fc9e688898f244c6906dde792a0948b9bac1f3acb3637db9c095482334"
)

// T-KDF-01 / REQ-SEC-021: the schedule is a deterministic function of its
// inputs, and independent of derivation order.
func TestScheduleDeterministic(t *testing.T) {
	a, _ := newSchedule(fixed(32, 1), fixed(32, 2), fixed(32, 3), fixed(32, 4))
	b, _ := newSchedule(fixed(32, 1), fixed(32, 2), fixed(32, 3), fixed(32, 4))
	if !bytes.Equal(a.prk, b.prk) || !bytes.Equal(a.kConf.Bytes(), b.kConf.Bytes()) {
		t.Fatal("schedule not deterministic")
	}
	a1e, a1m, _ := a.recordKeys(S2R, 1)
	// Derive an unrelated key from b first, to check order-independence.
	b.recordKeys(R2S, 9)
	b1e, b1m, _ := b.recordKeys(S2R, 1)
	if !bytes.Equal(a1e.Bytes(), b1e.Bytes()) || !bytes.Equal(a1m.Bytes(), b1m.Bytes()) {
		t.Fatal("record keys depend on derivation order")
	}
	// distinct channels and directions derive distinct keys
	c2e, _, _ := a.recordKeys(S2R, 2)
	if bytes.Equal(a1e.Bytes(), c2e.Bytes()) {
		t.Fatal("channel 1 and 2 share an enc key")
	}
	r1e, _, _ := a.recordKeys(R2S, 1)
	if bytes.Equal(a1e.Bytes(), r1e.Bytes()) {
		t.Fatal("s2r and r2s share an enc key")
	}
}

func pipePair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	c, s := net.Pipe()
	t.Cleanup(func() { c.Close(); s.Close() })
	return c, s
}

// T-HS-01 / REQ-SEC-018: a full handshake over a pipe yields two sessions that
// agree on every derived key.
func TestHandshakeHappyPath(t *testing.T) {
	cConn, sConn := pipePair(t)
	kPair := obs.NewSecret(fixed(32, 0x7e))
	id := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}

	type res struct {
		s   *Session
		err error
	}
	cCh := make(chan res, 1)
	go func() {
		s, err := ClientHandshake(obs.Ctx{}, cConn, kPair, id, 2*time.Second)
		cCh <- res{s, err}
	}()
	srv, serr := ServerHandshake(obs.Ctx{}, sConn, kPair, id, 2*time.Second)
	cr := <-cCh
	if serr != nil || cr.err != nil {
		t.Fatalf("handshake failed: server=%v client=%v", serr, cr.err)
	}
	if srv.SessionID != cr.s.SessionID {
		t.Fatal("session id disagreement")
	}
	for _, dir := range []Direction{S2R, R2S} {
		for _, ch := range []uint8{0, 1, 2} {
			ce, cm := cr.s.RecordKeys(dir, ch)
			se, sm := srv.RecordKeys(dir, ch)
			if !bytes.Equal(ce.Bytes(), se.Bytes()) || !bytes.Equal(cm.Bytes(), sm.Bytes()) {
				t.Fatalf("dir=%s ch=%d key disagreement", dir, ch)
			}
		}
	}
	if !bytes.Equal(srv.KChan.Bytes(), cr.s.KChan.Bytes()) {
		t.Fatal("K_chan disagreement")
	}
}

// T-HS-02 / REQ-SEC-019: a tampered SERVER_HELLO makes the client fail closed
// with E4001 and no session.
func TestHandshakeTamperedServerHello(t *testing.T) {
	cConn, sConn := pipePair(t)
	kPair := obs.NewSecret(fixed(32, 0x7e))
	id := [8]byte{9, 9, 9, 9, 9, 9, 9, 9}

	errCh := make(chan error, 1)
	go func() {
		_, err := ClientHandshake(obs.Ctx{}, cConn, kPair, id, time.Second)
		errCh <- err
	}()

	// Play a tampering server by hand: read CLIENT_HELLO, send a SERVER_HELLO
	// with a public key we flip one bit of.
	ch := make([]byte, clientHelloLen)
	if _, err := readFull(sConn, ch); err != nil {
		t.Fatal(err)
	}
	sh := make([]byte, serverHelloLen)
	sh[0] = protocolVersion
	copy(sh[1:], fixed(nonceLen, 0x22))
	pub := fixed(pubLen, 0x40)
	pub[0] ^= 0x01
	copy(sh[1+nonceLen:], pub)
	if _, err := sConn.Write(sh); err != nil {
		t.Fatal(err)
	}
	// Then a bogus SERVER_CONFIRM so the client reaches its verify step.
	sConn.Write(fixed(tagLen, 0x00))

	err := <-errCh
	if fault.GetCode(err) != fault.E4001 {
		t.Fatalf("tampered hello: err = %v, want E4001", err)
	}
}

// T-HS-03 / REQ-SEC-017: forward secrecy structure. Two handshakes with the same
// pairing secret derive unrelated session keys, because each uses a fresh
// ephemeral X25519 key. A recording of one session cannot decrypt the other.
func TestHandshakeForwardSecrecy(t *testing.T) {
	kPair := obs.NewSecret(fixed(32, 0x7e))
	id := [8]byte{1, 1, 1, 1, 1, 1, 1, 1}

	run := func() *Session {
		cConn, sConn := pipePair(t)
		out := make(chan *Session, 1)
		go func() {
			s, err := ClientHandshake(obs.Ctx{}, cConn, kPair, id, 2*time.Second)
			if err != nil {
				t.Errorf("client: %v", err)
			}
			out <- s
		}()
		srv, err := ServerHandshake(obs.Ctx{}, sConn, kPair, id, 2*time.Second)
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		<-out
		return srv
	}
	a, b := run(), run()
	ae, _ := a.RecordKeys(S2R, 1)
	be, _ := b.RecordKeys(S2R, 1)
	if bytes.Equal(ae.Bytes(), be.Bytes()) {
		t.Fatal("two sessions derived the same record key; no forward secrecy")
	}
}

// T-CHAN-01 / REQ-SEC-022: JoinChannel and AcceptChannel complete for a valid
// channel id within the negotiated count.
func TestChannelJoinHappyPath(t *testing.T) {
	sessA, sessB := handshakeSessions(t)

	rConn, sConn := pipePair(t)
	errCh := make(chan error, 1)
	go func() { errCh <- JoinChannel(obs.Ctx{}, rConn, sessA, 3) }()
	got, err := AcceptChannel(obs.Ctx{}, sConn, sessB, 4)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if got != 3 {
		t.Fatalf("accepted channel %d, want 3", got)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("join: %v", err)
	}
}

// T-CHAN-02 / REQ-SEC-023: a bad tag, an out-of-range id, channel 0, and a
// duplicate join are each E4002.
func TestChannelJoinRejections(t *testing.T) {
	_, sessB := handshakeSessions(t)

	// Drive AcceptChannel directly with crafted CHANNEL_JOIN bytes.
	newAccept := func(t *testing.T, msg []byte, n uint8) error {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		go func() {
			a.Write(msg)
			io.Copy(io.Discard, a) // drain any CHANNEL_ACCEPT reply
		}()
		_, err := AcceptChannel(obs.Ctx{}, b, sessB, n)
		return err
	}

	valid := craftJoin(sessB, 2, true)
	if err := newAccept(t, valid, 4); err != nil {
		t.Fatalf("control: valid join rejected: %v", err)
	}
	if err := newAccept(t, craftJoin(sessB, 2, false), 4); fault.GetCode(err) != fault.E4002 {
		t.Fatalf("bad tag: err = %v, want E4002", err)
	}
	if err := newAccept(t, craftJoin(sessB, 7, true), 4); fault.GetCode(err) != fault.E4002 {
		t.Fatalf("above N: err = %v, want E4002", err)
	}
	if err := newAccept(t, craftJoin(sessB, 0, true), 4); fault.GetCode(err) != fault.E4002 {
		t.Fatalf("channel 0: err = %v, want E4002", err)
	}
	// duplicate: channel 2 was accepted by the control call above.
	if err := newAccept(t, craftJoin(sessB, 2, true), 4); fault.GetCode(err) != fault.E4002 {
		t.Fatalf("duplicate: err = %v, want E4002", err)
	}
}

// --- helpers ---

func readFull(c net.Conn, b []byte) (int, error) {
	got := 0
	for got < len(b) {
		n, err := c.Read(b[got:])
		got += n
		if err != nil {
			return got, err
		}
	}
	return got, nil
}

func handshakeSessions(t *testing.T) (*Session, *Session) {
	t.Helper()
	cConn, sConn := pipePair(t)
	kPair := obs.NewSecret(fixed(32, 0x33))
	id := [8]byte{2, 4, 6, 8, 10, 12, 14, 16}
	out := make(chan *Session, 1)
	go func() {
		s, err := ClientHandshake(obs.Ctx{}, cConn, kPair, id, 2*time.Second)
		if err != nil {
			t.Errorf("client handshake: %v", err)
		}
		out <- s
	}()
	srv, err := ServerHandshake(obs.Ctx{}, sConn, kPair, id, 2*time.Second)
	if err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	return <-out, srv
}

func craftJoin(sess *Session, channelID uint8, goodTag bool) []byte {
	nonce := fixed(chanNonceLen, 0x5c)
	msg := make([]byte, 0, joinMsgLen)
	msg = append(msg, magic...)
	msg = append(msg, protocolVersion)
	msg = append(msg, sess.SessionID[:]...)
	msg = append(msg, channelID)
	msg = append(msg, nonce...)
	tag := channelTag(sess.KChan, "join", sess.SessionID, channelID, nonce)
	if !goodTag {
		tag[0] ^= 0x01
	}
	return append(msg, tag...)
}

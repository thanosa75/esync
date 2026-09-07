// Package handshake is esync's authenticated key agreement (ARCHITECTURE §6.2,
// §6.3, §6.4).
//
// The four handshake messages run in the clear on the control connection — they
// carry only public keys and nonces:
//
//	R -> S  CLIENT_HELLO   "ESYNC" | ver:u8 | session_id:8 | nonce_c:32 | Pc:32
//	S -> R  SERVER_HELLO   ver:u8 | nonce_s:32 | Ps:32
//	S -> R  SERVER_CONFIRM tag_s = HMAC(K_conf, "esync/v1 server confirm" || T)
//	R -> S  CLIENT_CONFIRM tag_c = HMAC(K_conf, "esync/v1 client confirm" || T)
//
// with Z = X25519(own_private, peer_public) (all-zero Z rejected),
// T = SHA-256(CLIENT_HELLO || SERVER_HELLO), and
// PRK = HKDF-Extract(salt = nonce_c||nonce_s, ikm = Z||K_pair). Both sides prove
// possession of the pairing secret before installing keys; the caller must not
// send a single application byte until ClientHandshake / ServerHandshake returns
// without error.
package handshake

import (
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"net"
	"time"

	"esync/internal/fault"
	"esync/internal/obs"
)

const (
	protocolVersion = 1
	magic           = "ESYNC"
	nonceLen        = 32
	pubLen          = 32
	tagLen          = 32

	clientHelloLen = len(magic) + 1 + 8 + nonceLen + pubLen // 78
	serverHelloLen = 1 + nonceLen + pubLen                  // 65

	// mismatchDelay is the fixed pause before the server closes on a session-id
	// mismatch, so it reveals nothing about which check failed (ARCHITECTURE §6.2).
	mismatchDelay = 250 * time.Millisecond
)

// Direction identifies one of the two per-direction key sets (ARCHITECTURE §6.3).
type Direction uint8

const (
	// S2R keys protect sender-to-receiver records.
	S2R Direction = iota
	// R2S keys protect receiver-to-sender records.
	R2S
)

func (d Direction) String() string {
	if d == R2S {
		return "r2s"
	}
	return "s2r"
}

// Peer returns the opposite direction.
func (d Direction) Peer() Direction {
	if d == R2S {
		return S2R
	}
	return R2S
}

// ClientHandshake runs the receiver side of the handshake over conn and returns
// the established session. deadline (the --handshake-timeout, default 5s) bounds
// the whole exchange via SetDeadline and is cleared on success.
func ClientHandshake(ctx obs.Ctx, conn net.Conn, kPair obs.Secret, sessionID [8]byte, deadline time.Duration) (*Session, error) {
	if deadline > 0 {
		_ = conn.SetDeadline(time.Now().Add(deadline))
	}

	curve := ecdh.X25519()
	priv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fault.Wrap(fault.E4007, "generate handshake key", "", err)
	}
	nonceC := make([]byte, nonceLen)
	if _, err := rand.Read(nonceC); err != nil {
		return nil, fault.Wrap(fault.E4007, "generate client nonce", "", err)
	}

	clientHello := clientHelloBytes(sessionID, nonceC, priv.PublicKey().Bytes())
	if _, err := conn.Write(clientHello); err != nil {
		return nil, ioFault("send client hello", err)
	}

	serverHello := make([]byte, serverHelloLen)
	if _, err := io.ReadFull(conn, serverHello); err != nil {
		return nil, ioFault("read server hello", err)
	}
	nonceS, ps, err := parseServerHello(serverHello)
	if err != nil {
		return nil, err
	}

	z, err := sharedSecret(curve, priv, ps)
	if err != nil {
		return nil, err
	}
	t := transcript(clientHello, serverHello)
	sch, err := newSchedule(nonceC, nonceS, z, kPair.Bytes())
	if err != nil {
		return nil, err
	}

	serverConfirm := make([]byte, tagLen)
	if _, err := io.ReadFull(conn, serverConfirm); err != nil {
		return nil, ioFault("read server confirm", err)
	}
	if !hmac.Equal(confirmTag(sch.kConf, "esync/v1 server confirm", t), serverConfirm) {
		return nil, fault.New(fault.E4001, "verify server confirm", "", nil)
	}
	if _, err := conn.Write(confirmTag(sch.kConf, "esync/v1 client confirm", t)); err != nil {
		return nil, ioFault("send client confirm", err)
	}

	if deadline > 0 {
		_ = conn.SetDeadline(time.Time{})
	}
	obs.Debug(ctx, "handshake complete", obs.F("span", "handshake"))
	return newSession(sessionID, protocolVersion, sch), nil
}

// ServerHandshake runs the sender side of the handshake over conn. On a
// session-id mismatch it closes after a fixed delay with a generic E4001 and
// does not reveal which check failed (ARCHITECTURE §6.2).
func ServerHandshake(ctx obs.Ctx, conn net.Conn, kPair obs.Secret, expectID [8]byte, deadline time.Duration) (*Session, error) {
	if deadline > 0 {
		_ = conn.SetDeadline(time.Now().Add(deadline))
	}

	clientHello := make([]byte, clientHelloLen)
	if _, err := io.ReadFull(conn, clientHello); err != nil {
		return nil, ioFault("read client hello", err)
	}
	gotID, nonceC, pc, err := parseClientHello(clientHello)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare(gotID[:], expectID[:]) != 1 {
		time.Sleep(mismatchDelay)
		return nil, fault.New(fault.E4001, "verify session id", "", nil)
	}

	curve := ecdh.X25519()
	priv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fault.Wrap(fault.E4007, "generate handshake key", "", err)
	}
	nonceS := make([]byte, nonceLen)
	if _, err := rand.Read(nonceS); err != nil {
		return nil, fault.Wrap(fault.E4007, "generate server nonce", "", err)
	}

	z, err := sharedSecret(curve, priv, pc)
	if err != nil {
		return nil, err
	}

	serverHello := serverHelloBytes(nonceS, priv.PublicKey().Bytes())
	t := transcript(clientHello, serverHello)
	sch, err := newSchedule(nonceC, nonceS, z, kPair.Bytes())
	if err != nil {
		return nil, err
	}

	if _, err := conn.Write(serverHello); err != nil {
		return nil, ioFault("send server hello", err)
	}
	if _, err := conn.Write(confirmTag(sch.kConf, "esync/v1 server confirm", t)); err != nil {
		return nil, ioFault("send server confirm", err)
	}

	clientConfirm := make([]byte, tagLen)
	if _, err := io.ReadFull(conn, clientConfirm); err != nil {
		return nil, ioFault("read client confirm", err)
	}
	if !hmac.Equal(confirmTag(sch.kConf, "esync/v1 client confirm", t), clientConfirm) {
		return nil, fault.New(fault.E4001, "verify client confirm", "", nil)
	}

	if deadline > 0 {
		_ = conn.SetDeadline(time.Time{})
	}
	obs.Debug(ctx, "handshake complete", obs.F("span", "handshake"))
	return newSession(expectID, protocolVersion, sch), nil
}

func sharedSecret(curve ecdh.Curve, priv *ecdh.PrivateKey, peerPub []byte) ([]byte, error) {
	pub, err := curve.NewPublicKey(peerPub)
	if err != nil {
		return nil, fault.Wrap(fault.E4001, "parse peer public key", "", err)
	}
	z, err := priv.ECDH(pub)
	if err != nil {
		return nil, fault.Wrap(fault.E4001, "x25519 exchange", "", err)
	}
	if isAllZero(z) {
		return nil, fault.New(fault.E4001, "x25519 exchange", "", nil)
	}
	return z, nil
}

func transcript(clientHello, serverHello []byte) []byte {
	h := sha256.New()
	h.Write(clientHello)
	h.Write(serverHello)
	return h.Sum(nil)
}

func confirmTag(kConf obs.Secret, label string, t []byte) []byte {
	m := hmac.New(sha256.New, kConf.Bytes())
	m.Write([]byte(label))
	m.Write(t)
	return m.Sum(nil)
}

func clientHelloBytes(sessionID [8]byte, nonceC, pc []byte) []byte {
	b := make([]byte, 0, clientHelloLen)
	b = append(b, magic...)
	b = append(b, protocolVersion)
	b = append(b, sessionID[:]...)
	b = append(b, nonceC...)
	b = append(b, pc...)
	return b
}

func serverHelloBytes(nonceS, ps []byte) []byte {
	b := make([]byte, 0, serverHelloLen)
	b = append(b, protocolVersion)
	b = append(b, nonceS...)
	b = append(b, ps...)
	return b
}

func parseClientHello(b []byte) (id [8]byte, nonceC, pc []byte, err error) {
	if len(b) != clientHelloLen || string(b[:len(magic)]) != magic {
		return id, nil, nil, fault.New(fault.E4001, "parse client hello", "", nil)
	}
	if v := b[len(magic)]; v != protocolVersion {
		return id, nil, nil, fault.Newf(fault.E2003, "parse client hello", "", nil,
			"peer protocol version %d, want %d", v, protocolVersion)
	}
	off := len(magic) + 1
	copy(id[:], b[off:off+8])
	off += 8
	nonceC = append([]byte(nil), b[off:off+nonceLen]...)
	off += nonceLen
	pc = append([]byte(nil), b[off:off+pubLen]...)
	return id, nonceC, pc, nil
}

func parseServerHello(b []byte) (nonceS, ps []byte, err error) {
	if len(b) != serverHelloLen {
		return nil, nil, fault.New(fault.E4001, "parse server hello", "", nil)
	}
	if v := b[0]; v != protocolVersion {
		return nil, nil, fault.Newf(fault.E2003, "parse server hello", "", nil,
			"peer protocol version %d, want %d", v, protocolVersion)
	}
	nonceS = append([]byte(nil), b[1:1+nonceLen]...)
	ps = append([]byte(nil), b[1+nonceLen:]...)
	return nonceS, ps, nil
}

func isAllZero(b []byte) bool {
	var v byte
	for _, c := range b {
		v |= c
	}
	return v == 0
}

// ioFault maps a handshake transport error to a fault: a timeout is E3004 (the
// handshake deadline lapsed), anything else is treated as an authentication
// failure (E4001), which is the only meaningful outcome of a broken handshake.
func ioFault(op string, err error) error {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return fault.Wrap(fault.E3004, op, "", err)
	}
	return fault.Wrap(fault.E4001, op, "", err)
}

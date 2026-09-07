package handshake

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"io"
	"net"

	"esync/internal/fault"
	"esync/internal/obs"
)

// Data-channel authentication (ARCHITECTURE §6.4). Each additional connection
// proves session membership before it is used:
//
//	R -> S  CHANNEL_JOIN   "ESYNC" | ver:u8 | session_id:8 | channel_id:u8 | nonce_ch:16
//	                       | tag = HMAC(K_chan, "join"   || session_id || channel_id || nonce_ch)
//	S -> R  CHANNEL_ACCEPT tag = HMAC(K_chan, "accept" || session_id || channel_id || nonce_ch)

const (
	chanNonceLen = 16
	joinMsgLen   = len(magic) + 1 + 8 + 1 + chanNonceLen + tagLen // 63
)

// JoinChannel is the receiver side: it sends CHANNEL_JOIN for channelID over
// conn and verifies the CHANNEL_ACCEPT reply. The caller must only call it after
// the sender has sent SESSION_READY.
func JoinChannel(ctx obs.Ctx, conn net.Conn, sess *Session, channelID uint8) error {
	nonceCh := make([]byte, chanNonceLen)
	if _, err := rand.Read(nonceCh); err != nil {
		return fault.Wrap(fault.E4007, "generate channel nonce", "", err)
	}

	msg := make([]byte, 0, joinMsgLen)
	msg = append(msg, magic...)
	msg = append(msg, protocolVersion)
	msg = append(msg, sess.SessionID[:]...)
	msg = append(msg, channelID)
	msg = append(msg, nonceCh...)
	msg = append(msg, channelTag(sess.KChan, "join", sess.SessionID, channelID, nonceCh)...)
	if _, err := conn.Write(msg); err != nil {
		return fault.Wrap(fault.E3006, "send channel join", "", err)
	}

	accept := make([]byte, tagLen)
	if _, err := io.ReadFull(conn, accept); err != nil {
		return fault.Wrap(fault.E3006, "read channel accept", "", err)
	}
	if !hmac.Equal(channelTag(sess.KChan, "accept", sess.SessionID, channelID, nonceCh), accept) {
		return fault.New(fault.E4002, "verify channel accept", "", nil)
	}
	obs.Trace(ctx, "channel joined", obs.F("chan", channelID))
	return nil
}

// AcceptChannel is the sender side: it reads one CHANNEL_JOIN from conn,
// validates it against negotiatedN and the session, replies with CHANNEL_ACCEPT,
// and returns the joined channel id. A duplicate channel id, an id of 0 or above
// negotiatedN, a bad tag, or a session-id mismatch is E4002 and the caller must
// close the socket.
func AcceptChannel(ctx obs.Ctx, conn net.Conn, sess *Session, negotiatedN uint8) (uint8, error) {
	msg := make([]byte, joinMsgLen)
	if _, err := io.ReadFull(conn, msg); err != nil {
		return 0, fault.Wrap(fault.E3006, "read channel join", "", err)
	}
	if string(msg[:len(magic)]) != magic || msg[len(magic)] != protocolVersion {
		return 0, fault.New(fault.E4002, "parse channel join", "", nil)
	}
	off := len(magic) + 1
	var gotID [8]byte
	copy(gotID[:], msg[off:off+8])
	off += 8
	channelID := msg[off]
	off++
	nonceCh := msg[off : off+chanNonceLen]
	off += chanNonceLen
	tag := msg[off:]

	if subtle.ConstantTimeCompare(gotID[:], sess.SessionID[:]) != 1 {
		return 0, fault.New(fault.E4002, "verify channel join session id", "", nil)
	}
	if channelID == 0 || channelID > negotiatedN {
		return 0, fault.Newf(fault.E4002, "validate channel id", "", nil,
			"channel %d outside 1..%d", channelID, negotiatedN)
	}
	if !hmac.Equal(channelTag(sess.KChan, "join", sess.SessionID, channelID, nonceCh), tag) {
		return 0, fault.New(fault.E4002, "verify channel join tag", "", nil)
	}

	sess.mu.Lock()
	dup := sess.accepted[channelID]
	if !dup {
		sess.accepted[channelID] = true
	}
	sess.mu.Unlock()
	if dup {
		return 0, fault.Newf(fault.E4002, "validate channel id", "", nil, "channel %d already joined", channelID)
	}

	if _, err := conn.Write(channelTag(sess.KChan, "accept", sess.SessionID, channelID, nonceCh)); err != nil {
		return 0, fault.Wrap(fault.E3006, "send channel accept", "", err)
	}
	obs.Trace(ctx, "channel accepted", obs.F("chan", channelID))
	return channelID, nil
}

func channelTag(kChan obs.Secret, label string, id [8]byte, channelID uint8, nonce []byte) []byte {
	m := hmac.New(sha256.New, kChan.Bytes())
	m.Write([]byte(label))
	m.Write(id[:])
	m.Write([]byte{channelID})
	m.Write(nonce)
	return m.Sum(nil)
}

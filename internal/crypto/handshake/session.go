package handshake

import (
	"sync"

	"esync/internal/obs"
)

// Session is the result of a completed handshake: the shared session id, the
// channel-auth key, the peer's protocol version, and lazily-derived per-channel
// per-direction record keys.
//
// PeerPlatform is deliberately absent: the handshake carries no platform field.
// The peer platform is negotiated later in SESSION_PARAMS / SESSION_READY and is
// the caller's to record.
type Session struct {
	SessionID   [8]byte
	KChan       obs.Secret
	PeerVersion uint8

	sch      *schedule
	mu       sync.Mutex
	keys     map[recKey][2]obs.Secret
	accepted map[uint8]bool
}

type recKey struct {
	dir Direction
	ch  uint8
}

func newSession(id [8]byte, peerVersion uint8, sch *schedule) *Session {
	return &Session{
		SessionID:   id,
		KChan:       sch.kChan,
		PeerVersion: peerVersion,
		sch:         sch,
		keys:        make(map[recKey][2]obs.Secret),
		accepted:    make(map[uint8]bool),
	}
}

// RecordKeys returns the AES-256 encryption key and the HMAC key for records on
// channel ch in direction dir (ARCHITECTURE §6.3). Keys are derived once and
// cached.
func (s *Session) RecordKeys(dir Direction, ch uint8) (kEnc, kMac obs.Secret) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := recKey{dir, ch}
	if v, ok := s.keys[k]; ok {
		return v[0], v[1]
	}
	e, m, err := s.sch.recordKeys(dir, ch)
	if err != nil {
		// Unreachable: HKDF-Expand of 32 bytes with SHA-256 never fails. Return
		// empty secrets, which fail loudly at AES init rather than silently.
		return obs.Secret{}, obs.Secret{}
	}
	s.keys[k] = [2]obs.Secret{e, m}
	return e, m
}

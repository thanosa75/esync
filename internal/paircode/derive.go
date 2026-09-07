package paircode

import (
	"crypto/hkdf"
	"crypto/sha256"

	"esync/internal/obs"
)

// Derived identifiers (§5.5). Both are one-way functions of the 128-bit secret.

// SessionID is SHA-256("esync/v1/session-id" || secret)[:8] — a non-secret
// 64-bit correlation handle both sides compute without a round trip.
func SessionID(secret obs.Secret) [8]byte {
	h := sha256.New()
	h.Write([]byte("esync/v1/session-id"))
	h.Write(secret.Bytes())
	sum := h.Sum(nil)
	var out [8]byte
	copy(out[:], sum[:8])
	return out
}

// KPair is HKDF-SHA256(ikm=secret, salt="esync/v1/pair", info="", L=32), the key
// that binds both handshake confirmations to possession of the code (§6.2).
func KPair(secret obs.Secret) obs.Secret {
	key, err := hkdf.Key(sha256.New, secret.Bytes(), []byte("esync/v1/pair"), "", 32)
	if err != nil {
		// Unreachable: L=32 is far below HKDF-SHA256's 255*32-byte ceiling.
		panic("paircode: HKDF failed: " + err.Error())
	}
	return obs.NewSecret(key)
}

// Package wire is the frame layer: the plaintext framing that sits inside every
// encrypted record (ARCHITECTURE §9), plus the cleartext handshake and
// channel-auth wire forms (§6.2, §6.4).
//
// It owns the FRAME codec — [msg_type u8 | body_len u32 BE | body] — and one
// struct per message row of the §9.2 table, with the exact field order of §9.3.
// It does NOT own the encrypted record layer (that is crypto/record); a frame is
// the plaintext a record carries.
//
// Every length field is checked against a declared maximum and against the
// remaining buffer before any allocation (REQ-SEC-014). Malformed input yields a
// *fault.Fault with an E5xxx code from the §14.2 catalogue, never a bare error.
package wire

import (
	"encoding/binary"
	"errors"

	"esync/internal/fault"
)

// Protocol constants shared by the frame and handshake layers.
const (
	// Magic prefixes CLIENT_HELLO and CHANNEL_JOIN (§6.2, §6.4).
	Magic = "ESYNC"
	// ProtocolVersion is the version byte this build speaks.
	ProtocolVersion uint8 = 0x01
	// FrameHeaderLen is the fixed frame prefix: msg_type u8 + body_len u32.
	FrameHeaderLen = 5
)

// Declared maxima, checked before allocation (REQ-SEC-014, §8.1 MAX_CT).
const (
	// MaxFramePlaintext bounds a whole frame: 1 MiB data payload + slack.
	MaxFramePlaintext = 1<<20 + 4096

	maxStr        = 65535 // a str/bytes length is a u16, so this is the ceiling
	maxPathBytes  = 4096
	maxDigest     = 64
	maxEntryCount = 1024 // §9.3 GROUP_MANIFEST entry_count is 1..1024
	maxIndexList  = 1024 // needed_count / rejected_count within one group
	maxChunkData  = 1 << 20
	maxBlob       = 4096 // nonce / tag / digest carried as a bytes field
)

// errTrunc is the internal sentinel a reader raises when the buffer runs out or a
// declared bound is exceeded. It never escapes the package: callers convert it to
// an E5xxx fault.
var errTrunc = errors.New("wire: truncated or out-of-bounds field")

// MsgType is the one-byte message discriminator (§9.2).
type MsgType uint8

// Message is implemented by every §9.3 body struct.
type Message interface {
	Type() MsgType
}

// Marshal encodes m into a complete frame: [msg_type | body_len | body] (§9.1).
func Marshal(m Message) ([]byte, error) {
	body, err := encodeBody(m)
	if err != nil {
		return nil, err
	}
	out := make([]byte, FrameHeaderLen+len(body))
	out[0] = byte(m.Type())
	binary.BigEndian.PutUint32(out[1:5], uint32(len(body)))
	copy(out[FrameHeaderLen:], body)
	return out, nil
}

// Unmarshal parses a frame from a record plaintext, dispatching on msg_type.
// body_len must equal len(plaintext)-5 (E5010); an unknown msg_type is E5011; a
// malformed body is E5003.
func Unmarshal(plaintext []byte) (Message, error) {
	if len(plaintext) < FrameHeaderLen || len(plaintext) > MaxFramePlaintext {
		return nil, fault.Newf(fault.E5010, "decode frame", "", nil,
			"plaintext length %d outside [%d,%d]", len(plaintext), FrameHeaderLen, MaxFramePlaintext)
	}
	t := MsgType(plaintext[0])
	bodyLen := binary.BigEndian.Uint32(plaintext[1:5])
	if int(bodyLen) != len(plaintext)-FrameHeaderLen {
		return nil, fault.Newf(fault.E5010, "decode frame", "", nil,
			"body_len %d != plaintext_len-5 (%d)", bodyLen, len(plaintext)-FrameHeaderLen)
	}
	return decodeBody(t, plaintext[FrameHeaderLen:])
}

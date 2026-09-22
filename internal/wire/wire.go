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

	"esync/internal/crypto/record"
	"esync/internal/fault"
)

// Protocol constants shared by the frame and handshake layers.
const (
	// Magic prefixes CLIENT_HELLO and CHANNEL_JOIN (§6.2, §6.4).
	Magic = "ESYNC"
	// ProtocolVersion is the version byte this build speaks.
	ProtocolVersion uint8 = 0x02
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

	// ManifestEntryFixedBytes is the wire cost of a ManifestEntry's fixed-width
	// fields, i.e. everything encodeManifestEntry writes except Path, Digest,
	// LinkTarget and HardlinkKey: EntryType(1) + Flags(2) + Path length-prefix(2)
	// + Size(8) + Mode(4) + MtimeSec(8) + MtimeNsec(4) + digest length-prefix(1).
	ManifestEntryFixedBytes = 1 + 2 + 2 + 8 + 4 + 8 + 4 + 1
	// ManifestEntryLinkTargetPrefix is the length-prefix cost of the optional
	// LinkTarget blob (present only when EntryType is symlink, §9.3).
	ManifestEntryLinkTargetPrefix = 2
	// ManifestEntryHardlinkKeyBytes is the cost of the optional HardlinkKey
	// field (present only when ManifestFlagHasHardlinkKey is set).
	ManifestEntryHardlinkKeyBytes = 8
	// MaxDigestBytes is the wire ceiling on a ManifestEntry digest: the largest
	// value any hash algorithm may ever produce on this wire (the u8 length
	// prefix could in principle carry up to 255, but §9.3 declares this the
	// protocol maximum).
	MaxDigestBytes = maxDigest
	// GroupManifestFixedBytes is the wire cost of a GROUP_MANIFEST's fixed
	// fields, i.e. everything but the entries themselves: GroupID(4) +
	// FirstFileID(8) + entry_count(2).
	GroupManifestFixedBytes = 4 + 8 + 2

	// manifestSafetyMargin is subtracted from the control-channel record
	// ceiling when deriving MaxGroupManifestEntryBytes, leaving headroom for
	// future GROUP_MANIFEST field growth beyond the worst-case PKCS#7 padding
	// (already accounted for via record.PadBlockSize).
	manifestSafetyMargin = 4096

	// MaxGroupManifestFrameBytes is the largest a GROUP_MANIFEST frame (frame
	// header + body) may be while still guaranteeing its control-record ct_len
	// stays under record.MaxCTControl after AES-CBC padding, with
	// manifestSafetyMargin bytes to spare.
	MaxGroupManifestFrameBytes = record.MaxCTControl - record.PadBlockSize - manifestSafetyMargin
	// MaxGroupManifestEntryBytes is the total encoded-entry byte budget
	// available to one GROUP_MANIFEST: MaxGroupManifestFrameBytes minus the
	// frame header and the message's own fixed fields. plan.Build's grouping
	// keeps a group's summed ManifestEntrySize under this so no group can ever
	// produce a control record the record layer rejects (R-13).
	MaxGroupManifestEntryBytes = MaxGroupManifestFrameBytes - FrameHeaderLen - GroupManifestFixedBytes
)

// ManifestEntrySize returns the exact encoded wire size of a ManifestEntry
// given its path length, a digest length (the caller decides how
// conservative to be — the real digest is not always known yet), and its
// optional fields. It mirrors encodeManifestEntry field-for-field so callers
// (plan's group packing) never have to guess the layout.
func ManifestEntrySize(pathLen, digestLen int, isSymlink bool, linkTargetLen int, hasHardlinkKey bool) int {
	n := ManifestEntryFixedBytes + pathLen + digestLen
	if isSymlink {
		n += ManifestEntryLinkTargetPrefix + linkTargetLen
	}
	if hasHardlinkKey {
		n += ManifestEntryHardlinkKeyBytes
	}
	return n
}

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

// Package paircode is the pairing-code format (ARCHITECTURE §5): the short
// bearer secret the sender prints and the user pastes into the receiver.
//
// A code is a Crockford Base32 encoding of a binary payload — version byte,
// header, 1..4 endpoints, a 128-bit root secret, and a CRC-32C — built by the
// sender and validated structurally, then by alphabet, then by checksum on the
// receiver before any socket is opened (§5.3). Decode performs NO I/O.
//
// The root secret is held in an obs.Secret so an accidental %v cannot leak it
// (§15.9); the derived session id is one-way and safe to log (§5.5, REQ-PAIR-009).
package paircode

import (
	"encoding/base32"
	"encoding/binary"
	"hash/crc32"
	"net/netip"

	"esync/internal/fault"
	"esync/internal/obs"
)

// Alphabet is Crockford Base32 (§5.2): no I, L, O, or U.
const Alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// Version is the code-format version byte (§5.1); 0x01 for this document.
const Version = 0x01

// SecretLen is the size of the root pairing secret in bytes (128 bits).
const SecretLen = 16

const (
	familyV4 = 0x04
	familyV6 = 0x06

	minPayloadLen = 1 + 1 + (1 + 4 + 2) + SecretLen + 4 // 29: single IPv4 endpoint
	maxEndpoints  = 4
	maxPayloadLen = 1 + 1 + maxEndpoints*(1+16+2) + SecretLen + 4
	// maxCodeLen bounds a cleaned code string before any allocation.
	maxCodeLen = 260
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
	// b32 is the Crockford alphabet with no padding; aliases are folded before use.
	b32 = base32.NewEncoding(Alphabet).WithPadding(base32.NoPadding)
)

// Endpoint is one address the receiver may dial (§5.2).
type Endpoint struct {
	Addr netip.Addr
	Port uint16
}

func (e Endpoint) usable() bool {
	return e.Addr.IsValid() && !e.Addr.IsUnspecified() && e.Port != 0
}

// Payload is the decoded content of a pairing code (§5.1).
type Payload struct {
	Endpoints  []Endpoint
	Secret     obs.Secret // 128-bit root secret; renders "[redacted]"
	Restricted bool       // sender was bound to a single interface (§5.4 step 4)
}

// Encode builds the canonical printed code for p: upper-case Crockford Base32
// with a '-' every five characters (§5.2). p must carry 1..4 endpoints and a
// 16-byte secret; those are guaranteed by the sender's construction.
func Encode(p *Payload) string {
	n := len(p.Endpoints)
	if n < 1 {
		n = 1
	}
	if n > maxEndpoints {
		n = maxEndpoints
	}

	var b []byte
	b = append(b, Version)
	header := byte(n-1) << 6
	if p.Restricted {
		header |= 1 << 3
	}
	b = append(b, header)
	for _, e := range p.Endpoints[:n] {
		if e.Addr.Is4() {
			b = append(b, familyV4)
			a := e.Addr.As4()
			b = append(b, a[:]...)
		} else {
			b = append(b, familyV6)
			a := e.Addr.As16()
			b = append(b, a[:]...)
		}
		b = binary.BigEndian.AppendUint16(b, e.Port)
	}
	var sec [SecretLen]byte
	copy(sec[:], p.Secret.Bytes())
	b = append(b, sec[:]...)
	b = binary.BigEndian.AppendUint32(b, crc32.Checksum(b, crcTable))

	return group(b32.EncodeToString(b))
}

// group upper-cases s and inserts a '-' every five characters.
func group(s string) string {
	out := make([]byte, 0, len(s)+len(s)/5)
	for i := 0; i < len(s); i++ {
		if i > 0 && i%5 == 0 {
			out = append(out, '-')
		}
		out = append(out, s[i])
	}
	return string(out)
}

// Decode parses a pairing code. It validates structure, then alphabet, then the
// CRC — any failure is E2001; an unknown version byte is E2002; a code with no
// usable endpoint is E2004. It performs no I/O (§5.2).
func Decode(code string) (*Payload, error) {
	cleaned := clean(code)
	if len(cleaned) < 47 || len(cleaned) > maxCodeLen {
		return nil, fault.Newf(fault.E2001, "decode pairing code", "", nil,
			"cleaned length %d out of range", len(cleaned))
	}
	for i := 0; i < len(cleaned); i++ {
		if alphabetIndex(cleaned[i]) < 0 {
			return nil, fault.Newf(fault.E2001, "decode pairing code", "", nil,
				"character %q not in the alphabet", cleaned[i])
		}
	}

	raw, err := b32.DecodeString(cleaned)
	if err != nil || len(raw) < minPayloadLen || len(raw) > maxPayloadLen {
		return nil, fault.New(fault.E2001, "decode pairing code", "", err)
	}
	// Reject a non-canonical encoding (e.g. non-zero trailing slack bits in the
	// final character): the last base32 symbol of a byte-unaligned payload
	// carries bits the payload does not use, and a substitution there would
	// otherwise slip past the CRC (§5.2 "canonical printed form").
	if b32.EncodeToString(raw) != cleaned {
		return nil, fault.New(fault.E2001, "decode pairing code", "", nil)
	}

	if raw[0] != Version {
		return nil, fault.Newf(fault.E2002, "decode pairing code", "", nil,
			"unknown code-format version 0x%02x", raw[0])
	}

	header := raw[1]
	if header&0b00110111 != 0 { // reserved bits MUST be 0 (§5.1)
		return nil, fault.New(fault.E2001, "decode pairing code", "", nil)
	}
	count := int(header>>6) + 1
	restricted := header&(1<<3) != 0

	pos := 2
	eps := make([]Endpoint, 0, count)
	for i := 0; i < count; i++ {
		if pos+1 > len(raw) {
			return nil, fault.New(fault.E2001, "decode pairing code", "", nil)
		}
		var alen int
		switch raw[pos] {
		case familyV4:
			alen = 4
		case familyV6:
			alen = 16
		default:
			return nil, fault.New(fault.E2001, "decode pairing code", "", nil)
		}
		pos++
		if pos+alen+2 > len(raw) {
			return nil, fault.New(fault.E2001, "decode pairing code", "", nil)
		}
		addr, ok := netip.AddrFromSlice(raw[pos : pos+alen])
		if !ok {
			return nil, fault.New(fault.E2001, "decode pairing code", "", nil)
		}
		pos += alen
		port := binary.BigEndian.Uint16(raw[pos : pos+2])
		pos += 2
		eps = append(eps, Endpoint{Addr: addr, Port: port})
	}

	if pos+SecretLen+4 != len(raw) {
		return nil, fault.New(fault.E2001, "decode pairing code", "", nil)
	}
	secret := make([]byte, SecretLen)
	copy(secret, raw[pos:pos+SecretLen])
	want := binary.BigEndian.Uint32(raw[len(raw)-4:])
	if crc32.Checksum(raw[:len(raw)-4], crcTable) != want {
		return nil, fault.New(fault.E2001, "decode pairing code", "", nil)
	}

	usable := false
	for _, e := range eps {
		if e.usable() {
			usable = true
			break
		}
	}
	if !usable {
		return nil, fault.New(fault.E2004, "decode pairing code", "", nil)
	}

	return &Payload{Endpoints: eps, Secret: obs.NewSecret(secret), Restricted: restricted}, nil
}

// clean strips separators and folds Crockford aliases to canonical upper case
// (§5.2): '-', spaces, tabs and newlines are removed; I/L → 1; O → 0.
func clean(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '-', ' ', '\t', '\n', '\r':
			continue
		}
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		switch c {
		case 'I', 'L':
			c = '1'
		case 'O':
			c = '0'
		}
		out = append(out, c)
	}
	return string(out)
}

func alphabetIndex(c byte) int {
	for i := 0; i < len(Alphabet); i++ {
		if Alphabet[i] == c {
			return i
		}
	}
	return -1
}

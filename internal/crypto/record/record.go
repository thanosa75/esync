// Package record is esync's encrypt-then-MAC record layer (ARCHITECTURE §8, §6.5).
//
// Every byte after the handshake, on every channel, is a sequence of records:
//
//	[ version 0x01 | channel_id u8 | seq u64 BE | ct_len u32 BE | iv 16 | ciphertext | mac 32 ]
//
// The 14-byte header is cleartext (framing needs it) but authenticated (it is
// inside the MAC). Encryption is AES-256-CBC with PKCS#7 padding; the MAC is
// HMAC-SHA-256 over bytes[0 : 30+ct_len], verified in constant time before any
// decryption is attempted. A fresh CSPRNG IV is drawn per record. Sequence
// numbers are per channel per direction, start at 0, and never wrap.
//
// A stream ends with one authenticated CLOSE record (a record whose plaintext is
// empty); ReadFrame reports it as io.EOF. A stream that stops without it is an
// E4005 fault (truncation).
package record

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math"
	"sync"

	"esync/internal/fault"
	"esync/internal/obs"
)

const (
	version   = 0x01
	headerLen = 14 // version(1) + channel_id(1) + seq(8) + ct_len(4)
	ivLen     = 16
	macLen    = 32
	prefixLen = headerLen + ivLen // bytes before the ciphertext
	blockSize = 16

	// MaxCTData is the ct_len ceiling on a data channel: 1 MiB of plaintext plus
	// one PKCS#7 padding block.
	MaxCTData = 1<<20 + blockSize
	// MaxCTControl is the ct_len ceiling on control channel 0: 256 KiB plus a pad block.
	MaxCTControl = 1<<18 + blockSize
)

func maxCT(channelID uint8) int {
	if channelID == 0 {
		return MaxCTControl
	}
	return MaxCTData
}

// Writer serialises frames onto an io.Writer as records for one channel and
// direction. Callers are expected to use one goroutine per Writer; it also
// serialises internally so an accidental concurrent write cannot corrupt the
// sequence counter.
type Writer struct {
	mu     sync.Mutex
	w      io.Writer
	kEnc   obs.Secret
	kMac   obs.Secret
	rand   io.Reader
	chID   uint8
	maxCT  int
	seq    uint64
	closed bool
}

// NewWriter wraps w for channelID with the given per-channel per-direction keys.
func NewWriter(w io.Writer, kEnc, kMac obs.Secret, channelID uint8) *Writer {
	return newWriter(w, kEnc, kMac, channelID, rand.Reader)
}

func newWriter(w io.Writer, kEnc, kMac obs.Secret, channelID uint8, r io.Reader) *Writer {
	return &Writer{w: w, kEnc: kEnc, kMac: kMac, rand: r, chID: channelID, maxCT: maxCT(channelID)}
}

// WriteFrame encrypts and MACs one frame and writes its record. The plaintext
// must be non-empty; the empty record is reserved for the authenticated close.
func (w *Writer) WriteFrame(plaintext []byte) error {
	if len(plaintext) == 0 {
		return fault.Newf(fault.E9004, "write record frame", "", nil,
			"empty frame; the empty record is reserved for Close")
	}
	return w.writeRecord(plaintext, false)
}

// Close writes the authenticated CLOSE record. It is idempotent.
func (w *Writer) Close() error {
	return w.writeRecord(nil, true)
}

func (w *Writer) writeRecord(plaintext []byte, isClose bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		if isClose {
			return nil
		}
		return fault.Newf(fault.E9004, "write record", "", nil, "writer already closed")
	}
	if w.seq == math.MaxUint64 {
		return fault.New(fault.E4006, "write record", "", nil)
	}

	padded := pkcs7Pad(plaintext)
	ctLen := len(padded)
	if ctLen > w.maxCT {
		return fault.Newf(fault.E5003, "write record", "", nil,
			"frame of %d bytes exceeds the channel maximum", len(plaintext))
	}

	buf := make([]byte, prefixLen+ctLen+macLen)
	buf[0] = version
	buf[1] = w.chID
	binary.BigEndian.PutUint64(buf[2:10], w.seq)
	binary.BigEndian.PutUint32(buf[10:14], uint32(ctLen))
	if _, err := io.ReadFull(w.rand, buf[headerLen:prefixLen]); err != nil {
		return fault.Wrap(fault.E4007, "draw record iv", "", err)
	}

	block, err := aes.NewCipher(w.kEnc.Bytes())
	if err != nil {
		return fault.Wrap(fault.E4007, "init record cipher", "", err)
	}
	cipher.NewCBCEncrypter(block, buf[headerLen:prefixLen]).CryptBlocks(buf[prefixLen:prefixLen+ctLen], padded)

	m := hmac.New(sha256.New, w.kMac.Bytes())
	m.Write(buf[:prefixLen+ctLen])
	copy(buf[prefixLen+ctLen:], m.Sum(nil))

	if _, err := w.w.Write(buf); err != nil {
		return fault.Wrap(fault.E3003, "write record", "", err)
	}
	w.seq++
	if isClose {
		w.closed = true
	}
	return nil
}

// Reader reads records for one channel and direction from an io.Reader and
// returns their frame plaintexts.
type Reader struct {
	r     io.Reader
	kEnc  obs.Secret
	kMac  obs.Secret
	chID  uint8
	maxCT int
	seq   uint64
	done  bool
}

// NewReader wraps r for channelID with the given per-channel per-direction keys.
func NewReader(r io.Reader, kEnc, kMac obs.Secret, channelID uint8) *Reader {
	return &Reader{r: r, kEnc: kEnc, kMac: kMac, chID: channelID, maxCT: maxCT(channelID)}
}

// ReadFrame reads and verifies the next record and returns its frame plaintext.
// A clean authenticated close returns io.EOF. Any protocol or integrity
// violation is a fault and the stream must not be used further.
func (r *Reader) ReadFrame() ([]byte, error) {
	if r.done {
		return nil, io.EOF
	}

	var h [headerLen]byte
	if _, err := io.ReadFull(r.r, h[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, fault.New(fault.E4005, "read record header", "", nil)
		}
		return nil, fault.Wrap(fault.E3003, "read record header", "", err)
	}

	// Validate the header before allocating anything (REQ-SEC-014).
	if h[0] != version {
		return nil, fault.Newf(fault.E5002, "read record", "", nil, "record version %d, want %d", h[0], version)
	}
	if h[1] != r.chID {
		return nil, fault.Newf(fault.E4003, "read record", "", nil, "record on channel %d, expected %d", h[1], r.chID)
	}
	ctLen := binary.BigEndian.Uint32(h[10:14])
	if ctLen < blockSize || ctLen > uint32(r.maxCT) || ctLen%blockSize != 0 {
		return nil, fault.Newf(fault.E5003, "read record", "", nil, "ct_len %d out of range", ctLen)
	}

	body := make([]byte, ivLen+int(ctLen)+macLen)
	if _, err := io.ReadFull(r.r, body); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, fault.New(fault.E4005, "read record body", "", nil)
		}
		return nil, fault.Wrap(fault.E3003, "read record body", "", err)
	}
	iv := body[:ivLen]
	ct := body[ivLen : ivLen+int(ctLen)]
	gotMAC := body[ivLen+int(ctLen):]

	m := hmac.New(sha256.New, r.kMac.Bytes())
	m.Write(h[:])
	m.Write(iv)
	m.Write(ct)
	if !hmac.Equal(m.Sum(nil), gotMAC) {
		return nil, fault.New(fault.E4003, "verify record mac", "", nil)
	}

	if seq := binary.BigEndian.Uint64(h[2:10]); seq != r.seq {
		return nil, fault.Newf(fault.E4004, "verify record sequence", "", nil,
			"record seq %d, expected %d", seq, r.seq)
	}

	block, err := aes.NewCipher(r.kEnc.Bytes())
	if err != nil {
		return nil, fault.Wrap(fault.E4007, "init record cipher", "", err)
	}
	pt := make([]byte, ctLen)
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(pt, ct)
	frame, err := pkcs7Unpad(pt)
	if err != nil {
		return nil, err
	}

	r.seq++
	if len(frame) == 0 {
		r.done = true
		return nil, io.EOF
	}
	return frame, nil
}

func pkcs7Pad(b []byte) []byte {
	pad := blockSize - len(b)%blockSize
	out := make([]byte, len(b)+pad)
	copy(out, b)
	for i := len(b); i < len(out); i++ {
		out[i] = byte(pad)
	}
	return out
}

func pkcs7Unpad(b []byte) ([]byte, error) {
	n := len(b)
	if n == 0 || n%blockSize != 0 {
		return nil, fault.Newf(fault.E5004, "unpad record", "", nil, "plaintext length %d not a block multiple", n)
	}
	pad := int(b[n-1])
	if pad == 0 || pad > blockSize || pad > n {
		return nil, fault.Newf(fault.E5004, "unpad record", "", nil, "invalid pad length %d", pad)
	}
	for _, c := range b[n-pad:] {
		if int(c) != pad {
			return nil, fault.New(fault.E5004, "unpad record", "", nil)
		}
	}
	return b[:n-pad], nil
}

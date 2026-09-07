package wire

import "encoding/binary"

// writer is an append-only big-endian encoder for frame bodies and handshake
// messages. It cannot fail: bodies are bounded and built from in-memory structs.
type writer struct {
	b []byte
}

func (w *writer) u8(v uint8)   { w.b = append(w.b, v) }
func (w *writer) u16(v uint16) { w.b = binary.BigEndian.AppendUint16(w.b, v) }
func (w *writer) u32(v uint32) { w.b = binary.BigEndian.AppendUint32(w.b, v) }
func (w *writer) u64(v uint64) { w.b = binary.BigEndian.AppendUint64(w.b, v) }
func (w *writer) i64(v int64)  { w.u64(uint64(v)) }

// str writes a u16 length followed by the UTF-8 bytes (§9.1).
func (w *writer) str(s string) {
	w.u16(uint16(len(s)))
	w.b = append(w.b, s...)
}

// blob writes a u16 length followed by raw bytes (the §9.1 "bytes" type).
func (w *writer) blob(p []byte) {
	w.u16(uint16(len(p)))
	w.b = append(w.b, p...)
}

// raw writes bytes with no length prefix (a fixed- or externally-sized field).
func (w *writer) raw(p []byte) { w.b = append(w.b, p...) }

// reader is a bounds-checked big-endian decoder. Once a read overruns the buffer
// or a declared maximum, err is latched and every subsequent read is a no-op, so
// callers test err once at the end.
type reader struct {
	b   []byte
	i   int
	err error
}

func (r *reader) fail() {
	if r.err == nil {
		r.err = errTrunc
	}
}

func (r *reader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if n < 0 || r.i+n > len(r.b) {
		r.fail()
		return false
	}
	return true
}

func (r *reader) u8() uint8 {
	if !r.need(1) {
		return 0
	}
	v := r.b[r.i]
	r.i++
	return v
}

func (r *reader) u16() uint16 {
	if !r.need(2) {
		return 0
	}
	v := binary.BigEndian.Uint16(r.b[r.i:])
	r.i += 2
	return v
}

func (r *reader) u32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(r.b[r.i:])
	r.i += 4
	return v
}

func (r *reader) u64() uint64 {
	if !r.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(r.b[r.i:])
	r.i += 8
	return v
}

func (r *reader) i64() int64 { return int64(r.u64()) }

// str reads a u16-prefixed UTF-8 string, rejecting a length above max or beyond
// the buffer before allocating (REQ-SEC-014).
func (r *reader) str(max int) string {
	n := int(r.u16())
	if r.err != nil {
		return ""
	}
	if n > max || !r.need(n) {
		r.fail()
		return ""
	}
	s := string(r.b[r.i : r.i+n])
	r.i += n
	return s
}

// blob reads a u16-prefixed raw byte field, copied out of the backing buffer so
// the caller may retain it.
func (r *reader) blob(max int) []byte {
	n := int(r.u16())
	if r.err != nil {
		return nil
	}
	if n > max || !r.need(n) {
		r.fail()
		return nil
	}
	if n == 0 {
		return nil
	}
	out := make([]byte, n)
	copy(out, r.b[r.i:r.i+n])
	r.i += n
	return out
}

// raw reads exactly n externally-sized bytes, copied out, with n bounded by max.
func (r *reader) raw(n, max int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > max || !r.need(n) {
		r.fail()
		return nil
	}
	if n == 0 {
		return nil
	}
	out := make([]byte, n)
	copy(out, r.b[r.i:r.i+n])
	r.i += n
	return out
}

func (r *reader) remaining() int {
	if r.err != nil {
		return 0
	}
	return len(r.b) - r.i
}

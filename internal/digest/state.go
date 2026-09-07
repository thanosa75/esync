package digest

import (
	"encoding"
	"hash"

	"esync/internal/fault"
)

// Marshal serialises a streaming hash's state (ARCHITECTURE §11.4) so a partially
// received file can resume hashing without re-reading its prefix. The caller
// pairs the returned bytes with the verified byte offset in its journal. md5 and
// sha256 support this; a hash that does not returns an E8004 fault so the caller
// falls back to a local re-read.
func Marshal(h hash.Hash) ([]byte, error) {
	m, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return nil, fault.Newf(fault.E8004, "marshal digest state", "", nil,
			"hash type %T does not support state marshalling", h)
	}
	b, err := m.MarshalBinary()
	if err != nil {
		return nil, fault.Wrap(fault.E8004, "marshal digest state", "", err)
	}
	return b, nil
}

// Unmarshal reconstructs a streaming hash of algorithm a from state produced by
// Marshal. A corrupt or incompatible state is an E8004 fault (torn journal
// record): the caller discards the checkpoint and re-fetches the file.
func Unmarshal(a Algo, state []byte) (hash.Hash, error) {
	h, err := New(a)
	if err != nil {
		return nil, err
	}
	u, ok := h.(encoding.BinaryUnmarshaler)
	if !ok {
		return nil, fault.Newf(fault.E8004, "unmarshal digest state", a.String(), nil,
			"hash type %T does not support state marshalling", h)
	}
	if err := u.UnmarshalBinary(state); err != nil {
		return nil, fault.Wrap(fault.E8004, "unmarshal digest state", a.String(), err)
	}
	return h, nil
}

package obs

import (
	"fmt"
	"io"
	"log/slog"
)

const redacted = "[redacted]"

// Secret wraps bytes that must never reach a log: the root secret, K_* keys, file
// content (ARCHITECTURE §15.9). Its String/GoString/Format/LogValue/MarshalJSON
// all render "[redacted]", so an accidental %v or a structured field cannot leak
// it. The plaintext is reachable only through Bytes, named so it is greppable.
type Secret struct {
	b []byte
}

// NewSecret wraps b. The slice is retained, not copied.
func NewSecret(b []byte) Secret { return Secret{b: b} }

// Bytes returns the underlying plaintext. This is the only accessor.
func (s Secret) Bytes() []byte { return s.b }

// Zero overwrites the underlying bytes.
func (s Secret) Zero() {
	for i := range s.b {
		s.b[i] = 0
	}
}

func (Secret) String() string   { return redacted }
func (Secret) GoString() string { return redacted }

func (Secret) Format(f fmt.State, _ rune) { io.WriteString(f, redacted) }

func (Secret) LogValue() slog.Value { return slog.StringValue(redacted) }

func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }

// RedactedString holds the pairing code (ARCHITECTURE §5.5, §15.9). It renders
// "[redacted]" everywhere; the real value is reachable only through Reveal, so
// callers that legitimately need it (printing the code to stdout) are greppable.
type RedactedString string

// Reveal returns the plaintext string.
func (r RedactedString) Reveal() string { return string(r) }

func (RedactedString) String() string   { return redacted }
func (RedactedString) GoString() string { return redacted }

func (RedactedString) Format(f fmt.State, _ rune) { io.WriteString(f, redacted) }

func (RedactedString) LogValue() slog.Value { return slog.StringValue(redacted) }

func (RedactedString) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }

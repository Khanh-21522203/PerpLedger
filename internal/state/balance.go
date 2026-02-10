// internal/state/balance.go
package state

import "github.com/google/uuid"

// Balance represents a user's collateral balance
type Balance struct {
	UserID   uuid.UUID
	Asset    string
	Total    int64 // Fixed-point: amount * 1e8
	Reserved int64 // Fixed-point: amount * 1e8 (locked for margins)
}

// Available returns free balance
func (b *Balance) Available() int64 {
	return b.Total - b.Reserved
}

// CanonicalBytes for deterministic hashing
func (b *Balance) CanonicalBytes() []byte {
	buf := make([]byte, 0, 64)

	// user_id (16 bytes)
	buf = append(buf, b.UserID[:]...)

	// asset (length-prefixed)
	buf = append(buf, byte(len(b.Asset)))
	buf = append(buf, []byte(b.Asset)...)

	// total (8 bytes LE)
	buf = appendInt64LE(buf, b.Total)

	// reserved (8 bytes LE)
	buf = appendInt64LE(buf, b.Reserved)

	return buf
}

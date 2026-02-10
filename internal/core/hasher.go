package core

import (
	"crypto/sha256"
	"encoding/binary"
)

const GenesisHashSeed = "PerpLedger:genesis:v1"

// StateHasher computes deterministic state hashes
type StateHasher struct {
	prevHash [32]byte
}

// NewStateHasher initializes with genesis hash
func NewStateHasher() *StateHasher {
	genesis := sha256.Sum256([]byte(GenesisHashSeed))
	return &StateHasher{
		prevHash: genesis,
	}
}

// ComputeHash calculates state_hash[N] = SHA-256(prev_hash || sequence || state_digest)
func (h *StateHasher) ComputeHash(sequence int64, stateDigest []byte) [32]byte {
	hasher := sha256.New()

	// Write prev_hash (32 bytes)
	hasher.Write(h.prevHash[:])

	// Write sequence (8 bytes LE)
	var seqBuf [8]byte
	binary.LittleEndian.PutUint64(seqBuf[:], uint64(sequence))
	hasher.Write(seqBuf[:])

	// Write state digest
	hasher.Write(stateDigest)

	var hash [32]byte
	copy(hash[:], hasher.Sum(nil))

	// Update prev_hash for next iteration
	h.prevHash = hash

	return hash
}

// GetPrevHash returns current chain tip
func (h *StateHasher) GetPrevHash() [32]byte {
	return h.prevHash
}

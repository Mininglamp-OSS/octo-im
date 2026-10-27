package clusterconfig

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Sender incarnation reservation.
//
// Forwarded Config proposals are correlated by (incarnation, attempt) tokens
// (see pkg/raft/raft/forward_token.go). Start reserves the incarnation in the
// Config Raft Pebble store while holding its directory lock and before Raft
// exists: it reads the last reserved value, increments it and persists the
// result with a synced write. Every process lifetime of a data directory
// therefore forwards under a strictly larger incarnation than all earlier ones,
// and a second process on the same directory fails on the lock before it can
// reserve or forward. Any read, decode, wrap or write failure fails Start.
//
// Boundary: the guarantee covers one data directory used in place. Restoring,
// rolling back or cloning the Config Raft data directory can roll back or
// duplicate the incarnation and is unsupported unless the restore procedure
// reserves a strictly newer incarnation before the node joins the network.

var (
	ErrSenderIncarnationCorrupt   = errors.New("clusterconfig: corrupt sender incarnation")
	ErrSenderIncarnationExhausted = errors.New("clusterconfig: sender incarnation exhausted")
)

const senderIncarnationSize = 8

// incarnationStore is the storage surface used by the reservation.
type incarnationStore interface {
	// senderIncarnation returns the stored value and whether it exists.
	senderIncarnation() ([]byte, bool, error)
	// saveSenderIncarnation durably writes the value before returning.
	saveSenderIncarnation([]byte) error
}

func reserveSenderIncarnation(st incarnationStore) (uint64, error) {
	data, found, err := st.senderIncarnation()
	if err != nil {
		return 0, fmt.Errorf("clusterconfig: read sender incarnation: %w", err)
	}
	var last uint64
	if found {
		if len(data) != senderIncarnationSize {
			return 0, fmt.Errorf("%w: %d bytes", ErrSenderIncarnationCorrupt, len(data))
		}
		// Zero is never written, so a stored zero is corruption.
		if last = binary.BigEndian.Uint64(data); last == 0 {
			return 0, fmt.Errorf("%w: zero", ErrSenderIncarnationCorrupt)
		}
	}
	if last == math.MaxUint64 {
		return 0, ErrSenderIncarnationExhausted
	}
	next := last + 1
	buf := make([]byte, senderIncarnationSize)
	binary.BigEndian.PutUint64(buf, next)
	if err := st.saveSenderIncarnation(buf); err != nil {
		return 0, fmt.Errorf("clusterconfig: persist sender incarnation: %w", err)
	}
	return next, nil
}

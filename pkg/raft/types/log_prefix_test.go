package types

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogPrefixChecksActualIndexAndDurableBoundary(t *testing.T) {
	logs := []Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 3}, {Index: 4, Term: 3}}
	read := func(start, end, _ uint64) ([]Log, error) { return logs[start-1 : end-1], nil }
	start := func(term uint32) (uint64, error) {
		for _, log := range logs {
			if log.Term == term {
				return log.Index, nil
			}
		}
		return 0, nil
	}
	for _, tc := range []struct {
		name         string
		next, stored uint64
		term         uint32
		cut          uint64
		reason       Reason
	}{
		{"matching-earlier-term", 3, 4, 1, 0, ReasonOk},
		{"matching-latest-term", 5, 4, 3, 0, ReasonOk},
		{"newer-leader-term", 5, 4, 2, 2, ReasonTruncate},
		{"newer-follower-term", 3, 4, 2, 1, ReasonTruncate},
		{"leader-append-not-durable", 5, 2, 3, 2, ReasonTruncate},
		{"empty-leader", 2, 0, 1, 0, ReasonTruncate},
		{"empty-follower", 1, 4, 0, 0, ReasonOk},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cut, reason, err := CheckLogPrefix(tc.next, tc.stored, tc.term, read, start)
			require.NoError(t, err)
			require.Equal(t, tc.cut, cut)
			require.Equal(t, tc.reason, reason)
		})
	}
	for _, failure := range []func(uint64, uint64, uint64) ([]Log, error){
		func(uint64, uint64, uint64) ([]Log, error) { return nil, errors.New("unavailable") },
		func(uint64, uint64, uint64) ([]Log, error) { return nil, nil },
		func(uint64, uint64, uint64) ([]Log, error) { return []Log{{Index: 1, Term: 3}}, nil },
	} {
		_, reason, err := CheckLogPrefix(5, 4, 3, failure, start)
		require.Error(t, err)
		require.Equal(t, ReasonError, reason, "a missing durable entry cannot certify a prefix")
	}
}

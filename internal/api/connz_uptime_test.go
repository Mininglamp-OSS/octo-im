package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConnUptimeSupportsLegacySecondsAndSessionFenceNanoseconds(t *testing.T) {
	legacy := time.Unix(1_700_000_000, 0)
	precise := time.Unix(1_700_000_000, 123_456_789)

	require.Equal(t, legacy, connUptime(uint64(legacy.Unix())))
	require.Equal(t, precise, connUptime(uint64(precise.UnixNano())))
}

package api

import (
	"sort"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"

	"github.com/stretchr/testify/require"
)

func TestConnUptimeSupportsLegacySecondsAndSessionFenceNanoseconds(t *testing.T) {
	legacy := time.Unix(1_700_000_000, 0)
	precise := time.Unix(1_700_000_000, 123_456_789)

	require.Equal(t, legacy, connUptime(uint64(legacy.Unix())))
	require.Equal(t, precise, connUptime(uint64(precise.UnixNano())))
}

func TestConnUptimeSortNormalizesMixedVersionUnits(t *testing.T) {
	legacy := &connzResp{Conn: &eventbus.Conn{Uptime: 1_700_000_000}}
	olderPrecise := &connzResp{Conn: &eventbus.Conn{Uptime: uint64(time.Unix(1_699_999_999, 123).UnixNano())}}
	newerPrecise := &connzResp{Conn: &eventbus.Conn{Uptime: uint64(time.Unix(1_700_000_001, 456).UnixNano())}}
	conns := []*connzResp{legacy, newerPrecise, olderPrecise}
	sort.Sort(byUptime{Conns: conns})
	require.Equal(t, []*connzResp{olderPrecise, legacy, newerPrecise}, conns)
	sort.Sort(byUptimeDesc{Conns: conns})
	require.Equal(t, []*connzResp{newerPrecise, legacy, olderPrecise}, conns)
}

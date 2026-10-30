package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdvertiseAddrRejectsScopedUnspecifiedIP(t *testing.T) {
	for _, addr := range []string{
		"[::%lo]:11110",
		"[0:0:0:0:0:0:0:0%eth0]:11110",
		"tcp://[::%lo]:11110",
		"[::ffff:0.0.0.0]:11110",
		"[::ffff:0.0.0.0%lo]:11110",
	} {
		t.Run(addr, func(t *testing.T) {
			require.ErrorContains(t, ValidateAdvertiseAddr(addr), "bind-all")
			require.ErrorContains(t, ValidateJoinAddr(addr), "bind-all")
		})
	}
}

func TestAdvertiseAddrPreservesUsableIPZones(t *testing.T) {
	for _, addr := range []string{"[::1%lo]:11110", "[fe80::1%eth0]:11110", "tcp://[fe80::1234%3]:65535", "[::ffff:127.0.0.1]:11110"} {
		t.Run(addr, func(t *testing.T) {
			require.NoError(t, ValidateAdvertiseAddr(addr))
			require.NoError(t, ValidateJoinAddr(addr))
		})
	}
}

func TestAdvertiseAddrRejectsMalformedScopedIP(t *testing.T) {
	for _, addr := range []string{"[::%]:11110", "[not-an-ip%lo]:11110", "[2001:db8:::1%lo]:11110"} {
		t.Run(addr, func(t *testing.T) {
			require.Error(t, ValidateAdvertiseAddr(addr))
			require.Error(t, ValidateJoinAddr(addr))
		})
	}
}

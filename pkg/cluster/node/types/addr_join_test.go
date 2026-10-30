package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateJoinAddr(t *testing.T) {
	for _, addr := range []string{"", " ", "\t\n", "　"} {
		t.Run("empty_"+addr, func(t *testing.T) {
			require.NoError(t, ValidateAdvertiseAddr(addr), "bootstrap keeps its empty-address exception")
			require.ErrorContains(t, ValidateJoinAddr(addr), "required", "new members must advertise an address")
		})
	}
	for _, addr := range []string{"node.internal:1", "node.internal:65535", "127.0.0.1:11110", "[::1]:11110", "tcp://node.internal:11110"} {
		t.Run(addr, func(t *testing.T) {
			require.NoError(t, ValidateJoinAddr(addr))
		})
	}
	for _, addr := range []string{"node.internal:abc", "node.internal:0", "node.internal:-1", "node.internal:65536", "node.internal: 11110", "node.internal:11110 ", "0.0.0.0:11110", "[::]:11110"} {
		t.Run(addr, func(t *testing.T) {
			require.Error(t, ValidateJoinAddr(addr))
		})
	}
}

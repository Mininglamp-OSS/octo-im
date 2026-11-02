package wkdb

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChannelConfigCloneDoesNotShareMembership(t *testing.T) {
	original := ChannelClusterConfig{Replicas: []uint64{1, 3, 2}, Learners: []uint64{4}}
	clone := original.Clone()
	clone.Replicas[1] = 4
	clone.Learners[0] = 5
	require.Equal(t, []uint64{1, 3, 2}, original.Replicas)
	require.Equal(t, []uint64{4}, original.Learners)
}

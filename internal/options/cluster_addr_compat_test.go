package options

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigureLegacyUnusedServerAddr(t *testing.T) {
	vp := viper.New()
	vp.Set("rootDir", t.TempDir())
	vp.Set("cluster.nodeId", uint64(1))
	vp.Set("cluster.initNodes", []string{"1@127.0.0.1:11110"})
	vp.Set("cluster.serverAddr", "0.0.0.0:11110")
	opts := New()
	require.NotPanics(t, func() { opts.ConfigureWithViper(vp) })
	require.Len(t, opts.Cluster.InitNodes, 1)
	require.Equal(t, "127.0.0.1:11110", opts.Cluster.InitNodes[0].ServerAddr)
}

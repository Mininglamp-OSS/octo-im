package options

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestSubscriberRecoveryDefaultsEnabledAndCanBeDisabled(t *testing.T) {
	opts := New()
	opts.ConfigureWithViper(viper.New())
	require.True(t, opts.SubscriberRecovery.Enabled)
	require.False(t, opts.SubscriberRecovery.AsyncTargetEnabled)
	require.False(t, opts.SubscriberRecovery.GroupCommit.Enabled)
	require.Equal(t, 32, opts.SubscriberRecovery.GroupCommit.MaxCount)

	v := viper.New()
	v.Set("subscriberRecovery.enabled", false)
	opts = New()
	opts.ConfigureWithViper(v)
	require.False(t, opts.SubscriberRecovery.Enabled)

	v.Set("subscriberRecovery.asyncTarget.enabled", true)
	opts = New()
	opts.ConfigureWithViper(v)
	require.True(t, opts.SubscriberRecovery.AsyncTargetEnabled)
}

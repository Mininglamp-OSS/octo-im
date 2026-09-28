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

	v := viper.New()
	v.Set("subscriberRecovery.enabled", false)
	opts = New()
	opts.ConfigureWithViper(v)
	require.False(t, opts.SubscriberRecovery.Enabled)
}

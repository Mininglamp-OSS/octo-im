package trace

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestOnlineUserCountExportsCurrentGaugeWithExistingName(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter, err := prometheusexporter.New(prometheusexporter.WithRegisterer(registry))
	require.NoError(t, err)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	oldMeter := meter
	meter = provider.Meter("online-count-test")
	t.Cleanup(func() {
		meter = oldMeter
		require.NoError(t, provider.Shutdown(context.Background()))
	})
	var live atomic.Int64
	live.Store(126)
	app := newAppMetrics(NewOptions(WithOnlineUserCount(live.Load)))
	// A configured observer must be authoritative even if an older caller tries
	// to update the legacy manual value.
	app.OnlineUserCountSet(203)
	app.OnlineUserCountAdd(1)
	for _, expected := range []int64{126, 0, 2, 1} {
		live.Store(expected)
		families, err := registry.Gather()
		require.NoError(t, err)
		found := false
		for _, family := range families {
			if family.GetName() != "app_online_user_count_total" {
				continue
			}
			found = true
			require.Equal(t, "GAUGE", family.GetType().String())
			require.Len(t, family.Metric, 1)
			require.Equal(t, float64(expected), family.Metric[0].GetGauge().GetValue())
		}
		require.True(t, found, "existing dashboards must retain the same Prometheus series name")
	}
}

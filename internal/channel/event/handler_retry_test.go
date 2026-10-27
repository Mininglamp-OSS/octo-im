package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
)

func TestDistributionPendingWorkSurvivesClockRollback(t *testing.T) {
	previous := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = previous })
	c := &channelHandler{lastActive: fasttime.UnixTimestamp() + 3600}
	if c.isTimeout() {
		t.Fatal("an idle handler with no queue must preserve the fix1 clock-rollback guard")
	}
	c.pending.retryEvents = []*eventbus.Event{{Type: eventbus.EventChannelDistribute}}
	if c.isTimeout() {
		t.Fatal("a retained distribution task must keep its handler alive")
	}
}

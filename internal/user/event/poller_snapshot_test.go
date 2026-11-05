package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
)

func TestPollerSnapshotReuseReleasesRemovedHandlers(t *testing.T) {
	previous := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = previous })

	p, handlers := newConnectionStatsPoller(4096, 1)
	for _, h := range handlers {
		h.processing.Store(false)
	}
	p.handleEvents()
	if len(p.loopHandlers) != 0 || cap(p.loopHandlers) < len(handlers) {
		t.Fatal("event loop did not retain its snapshot storage")
	}
	backing := p.loopHandlers[:cap(p.loopHandlers)]
	checkCleared := func() {
		t.Helper()
		if len(p.loopHandlers) != 0 {
			t.Fatal("completed traversal left entries in the next snapshot")
		}
		for _, h := range backing {
			if h != nil {
				t.Fatal("snapshot storage retains a handler after traversal")
			}
		}
	}
	checkCleared()

	// A smaller snapshot must not leave references from the earlier peak.
	for i := 1; i < len(handlers); i += 2 {
		p.waitlist.remove(handlers[i].Uid)
	}
	p.tick()
	checkCleared()
	p.handleEvents()
	checkCleared()
	if cap(p.loopHandlers) != len(backing) || &p.loopHandlers[:cap(p.loopHandlers)][0] != &backing[0] {
		t.Fatal("nonempty poller replaced reusable snapshot storage")
	}
	if got := p.allConnCount(); got != len(handlers)/2 {
		t.Fatalf("connections after removal = %d, want %d", got, len(handlers)/2)
	}

	for i := 0; i < len(handlers); i += 2 {
		p.waitlist.remove(handlers[i].Uid)
	}
	p.handleEvents()
	if p.loopHandlers != nil {
		t.Fatal("empty poller should release peak snapshot capacity")
	}
}

package event

import (
	"fmt"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
)

func TestAllConnCountConcurrentReaders(t *testing.T) {
	for _, tc := range []struct {
		name        string
		users       int
		connections int
	}{
		{name: "empty"},
		{name: "one_user_two_connections", users: 1, connections: 2},
		{name: "small_list", users: 128, connections: 1},
		{name: "large_list", users: 2048, connections: 2},
	} {
		for _, withEvents := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/events=%t", tc.name, withEvents), func(t *testing.T) {
				p, _ := newConnectionStatsPoller(tc.users, tc.connections)
				want := tc.users * tc.connections
				const iterations = 100
				readers := 4
				var tasks []func()
				if withEvents {
					readers = 1
					tasks = append(tasks, func() {
						for i := 0; i < iterations; i++ {
							// These calls are serial in the real event loop as well.
							p.handleEvents()
							p.tick()
						}
					})
				}
				for reader := 0; reader < readers; reader++ {
					tasks = append(tasks, func() {
						for i := 0; i < iterations; i++ {
							if got := p.allConnCount(); got != want {
								t.Errorf("connection count = %d, want %d", got, want)
								return
							}
						}
					})
				}
				runConnectionStatsTasks(tasks...)
			})
		}
	}
}

func TestAllConnCountDuringUserAndConnectionChanges(t *testing.T) {
	const users, iterations = 64, 100
	p, handlers := newConnectionStatsPoller(users, 1)
	transient := newConnectionStatsHandler(p, "transient", 2)
	tasks := []func(){
		func() {
			for i := 0; i < iterations; i++ {
				p.handleEvents()
				p.tick()
			}
		},
		func() {
			for i := 0; i < iterations; i++ {
				for _, h := range handlers {
					conn := &eventbus.Conn{
						Uid: h.Uid, NodeId: 1, ConnId: 2,
						LastActive: fasttime.UnixTimestamp(),
					}
					h.conns.addOrUpdateConn(conn)
					h.conns.remove(conn)
				}
			}
		},
		func() {
			for i := 0; i < iterations; i++ {
				p.waitlist.push(transient)
				p.waitlist.remove(transient.Uid)
			}
		},
	}
	for reader := 0; reader < 4; reader++ {
		tasks = append(tasks, func() {
			for i := 0; i < iterations; i++ {
				// Collection and per-user counting are not one atomic snapshot.
				// Every retained user always has one connection and at most two.
				if got := p.allConnCount(); got < users || got > users*2+2 {
					t.Errorf("connection count outside valid bounds: %d", got)
					return
				}
			}
		})
	}
	runConnectionStatsTasks(tasks...)
	if got := p.allConnCount(); got != users {
		t.Fatalf("connection count after changes = %d, want %d", got, users)
	}
	if got := p.allUserCount(); got != users {
		t.Fatalf("user count after changes = %d, want %d", got, users)
	}
}

func BenchmarkAllConnCount(b *testing.B) {
	for _, users := range []int{128, 2048} {
		b.Run(fmt.Sprintf("users=%d", users), func(b *testing.B) {
			p, _ := newConnectionStatsPoller(users, 2)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := p.allConnCount(); got != users*2 {
					b.Fatalf("connection count = %d, want %d", got, users*2)
				}
			}
		})
	}
}

func newConnectionStatsPoller(users, connections int) (*poller, []*userHandler) {
	p := &poller{
		waitlist:  newLinkedList(),
		eventPool: &EventPool{handler: &mockUserEventHandler{}},
	}
	handlers := make([]*userHandler, 0, users)
	for i := 0; i < users; i++ {
		h := newConnectionStatsHandler(p, fmt.Sprintf("stats-%d", i), connections)
		p.waitlist.push(h)
		handlers = append(handlers, h)
	}
	return p, handlers
}

func newConnectionStatsHandler(p *poller, uid string, connections int) *userHandler {
	h := newUserHandler(uid, p)
	// Exercise the real list collection, traversal and cleanup without dispatching
	// business events to a worker pool or involving cluster routing.
	h.processing.Store(true)
	for i := 0; i < connections; i++ {
		h.conns.addOrUpdateConn(&eventbus.Conn{
			Uid: uid, NodeId: 1, ConnId: int64(i + 1),
			LastActive: fasttime.UnixTimestamp(),
		})
	}
	return h
}

func runConnectionStatsTasks(tasks ...func()) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go func(task func()) {
			defer wg.Done()
			<-start
			task()
		}(task)
	}
	close(start)
	wg.Wait()
}

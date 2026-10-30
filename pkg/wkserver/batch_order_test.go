package wkserver

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/panjf2000/gnet/v2"
)

// Block an older response at dispatch and see whether the later notification
// can overtake it despite MessagePoolOn=false (the cluster's real setting).
func TestBatchHonorsSynchronousDispatch(t *testing.T) {
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	finished := make(chan struct{}, 2)
	var suspended atomic.Bool
	var release sync.Once
	defer release.Do(func() { close(releaseFirst) })
	s := New("tcp://127.0.0.1:0", WithMessagePoolOn(false))
	defer s.messagePool.Release()
	defer s.requestPool.Release()
	s.OnMessage(func(_ gnet.Conn, m *proto.Message) {
		if m.Id == 1 {
			close(firstEntered)
			<-releaseFirst
			suspended.Store(true)
		} else {
			<-firstEntered
			close(secondEntered)
			suspended.Store(false)
		}
		finished <- struct{}{}
	})
	b := &proto.BatchMessage{Count: 2, Messages: []*proto.Message{{Id: 1, MsgType: 1, Content: []byte("older suspended response")}, {Id: 2, MsgType: 1, Content: []byte("newer wake notification")}}}
	data, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	dispatched := make(chan struct{})
	go func() { s.handleBatchMessage(nil, data); close(dispatched) }()
	select {
	case <-firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first callback did not start")
	}
	overtook := false
	select {
	case <-secondEntered:
		overtook = true
		// Ensure the second callback has completed its update before releasing
		// the old response; the scheduler cannot reverse the intended proof.
		<-finished
	case <-time.After(time.Second):
	}
	release.Do(func() { close(releaseFirst) })
	wantFinished := 2
	if overtook {
		wantFinished = 1
	}
	for i := 0; i < wantFinished; i++ {
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Fatal("callback did not finish")
		}
	}
	<-dispatched
	if overtook {
		t.Errorf("MessagePoolOn=false ignored: callback 2 completed before blocked callback 1; modeled wake then stale suspend leaves suspended=%v", suspended.Load())
	}
}

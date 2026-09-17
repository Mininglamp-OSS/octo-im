package wkdb

import (
	"testing"
	"time"
)

func TestAddSubscribersUsesChannelGroupCommit(t *testing.T) {
	wk, batchDB := newControlledSubscriberBatchDB(t, false)

	assertSubscriberOperationUsesGroupCommit(t, batchDB, func() error {
		return wk.AddSubscribers("channel-1", 2, []Member{{Uid: "uid-1"}})
	})

	exists, err := wk.ExistSubscriber("channel-1", 2, "uid-1")
	if err != nil {
		t.Fatalf("ExistSubscriber() error = %v", err)
	}
	if !exists {
		t.Fatal("subscriber was not persisted by the group commit")
	}
}

func TestRemoveSubscribersUsesChannelGroupCommit(t *testing.T) {
	wk, batchDB := newControlledSubscriberBatchDB(t, true)

	assertSubscriberOperationUsesGroupCommit(t, batchDB, func() error {
		return wk.RemoveSubscribers("channel-1", 2, []string{"uid-1"})
	})

	exists, err := wk.ExistSubscriber("channel-1", 2, "uid-1")
	if err != nil {
		t.Fatalf("ExistSubscriber() error = %v", err)
	}
	if exists {
		t.Fatal("subscriber was not removed by the group commit")
	}
}

func newControlledSubscriberBatchDB(t *testing.T, seedSubscriber bool) (*wukongDB, *BatchDB) {
	t.Helper()

	wk := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	if err := wk.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := wk.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	if seedSubscriber {
		if err := wk.AddSubscribers("channel-1", 2, []Member{{Uid: "uid-1"}}); err != nil {
			t.Fatalf("AddSubscribers() setup error = %v", err)
		}
	}

	wk.wkdbs[0].Stop()
	batchDB := NewBatchDB(0, wk.dbs[0])
	wk.wkdbs[0] = batchDB

	return wk, batchDB
}

func assertSubscriberOperationUsesGroupCommit(t *testing.T, batchDB *BatchDB, operation func() error) {
	t.Helper()

	errCh := make(chan error, 1)
	go func() {
		errCh <- operation()
	}()

	var batch *Batch
	select {
	case err := <-errCh:
		t.Fatalf("operation returned before the group commit worker ran: %v", err)
	case batch = <-batchDB.batchChan:
	case <-time.After(time.Second):
		t.Fatal("operation did not enqueue a group commit batch")
	}

	select {
	case err := <-errCh:
		t.Fatalf("operation returned before its group commit completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	batchDB.executeBatch([]*Batch{batch})

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("operation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("operation did not return after its group commit completed")
	}
}

package wkdb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// Run with -race: the worker releases each Batch immediately after notifying
// its caller. CommitWait must retain its own channel even after that release.
func TestBatchCommitWaitConcurrentCompletion(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batchDB := NewBatchDB(0, db)
	batchDB.Start()
	t.Cleanup(func() {
		batchDB.Stop()
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})

	const writers, commits = 16, 100
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for commit := range commits {
				batch := batchDB.NewBatch()
				batch.Set([]byte(fmt.Sprintf("%d/%d", writer, commit)), []byte("persisted"))
				if err := batch.CommitWait(); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("CommitWait lost a completion notification")
	}

	for writer := range writers {
		for commit := range commits {
			key := []byte(fmt.Sprintf("%d/%d", writer, commit))
			value, closer, err := db.Get(key)
			if err != nil {
				t.Fatalf("read %q after CommitWait: %v", key, err)
			}
			matches := string(value) == "persisted"
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
			if !matches {
				t.Fatalf("incorrect value for %q after CommitWait", key)
			}
		}
	}
}

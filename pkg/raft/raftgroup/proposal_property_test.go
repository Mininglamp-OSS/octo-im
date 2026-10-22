package raftgroup

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// R4 seeds. Each seed fixes every goroutine's proposal sizes, deadlines and
// the loop and apply pauses; scheduling still varies between runs.
var admissionPropertySeeds = []int64{20441, 20442, 20443}

const (
	propertyProposers = 64
	propertyProposals = 2000
)

type proposalRecord struct {
	ids   []uint64
	resps types.ProposeRespSet
	err   error
}

// pausingStorage delays some applies to vary the interleaving.
type pausingStorage struct {
	*memStorage
	mu  sync.Mutex
	rnd *rand.Rand
}

func (s *pausingStorage) Apply(key string, logs []types.Log) error {
	s.mu.Lock()
	pause := s.rnd.Intn(32) == 0
	d := time.Duration(s.rnd.Intn(300)) * time.Microsecond
	s.mu.Unlock()
	if pause {
		time.Sleep(d)
	}
	return s.memStorage.Apply(key, logs)
}

// R4: random interleaving of proposals, deadlines and loop pauses.
func TestAdmissionInterleavingProperty(t *testing.T) {
	for _, seed := range admissionPropertySeeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runAdmissionProperty(t, seed)
		})
	}
}

func runAdmissionProperty(t *testing.T, seed int64) {
	g := newAdmissionGroup(t, []uint64{1}, false)
	storage := &pausingStorage{memStorage: g.storage, rnd: rand.New(rand.NewSource(seed))}
	g.opts.Storage = storage
	loopRnd := rand.New(rand.NewSource(seed - 1)) // only used on the loop
	g.setBeforeAdmit(func(uint64) {
		if loopRnd.Intn(64) == 0 {
			time.Sleep(time.Duration(loopRnd.Intn(500)) * time.Microsecond)
		}
	})
	require.NoError(t, g.Start())

	records := make([][]proposalRecord, propertyProposers)
	var wg sync.WaitGroup
	for p := 0; p < propertyProposers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(seed*1000 + int64(p)))
			out := make([]proposalRecord, 0, propertyProposals)
			for i := 0; i < propertyProposals; i++ {
				n := 1 + rnd.Intn(4)
				rec := proposalRecord{}
				for j := 0; j < n; j++ {
					rec.ids = append(rec.ids, uint64(p+1)*10_000_000+uint64(i)*10+uint64(j))
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rnd.Intn(5001))*time.Microsecond)
				rec.resps, rec.err = g.propose(ctx, rec.ids...)
				cancel()
				out = append(out, rec)
			}
			records[p] = out
		}(p)
	}
	wg.Wait()

	// Admitted entries whose callers gave up are still committed and applied.
	stable := 0
	for deadline := time.Now().Add(20 * time.Second); stable < 5; {
		if time.Now().After(deadline) {
			t.Fatalf("seed %d: apply did not catch up: stored %d applied %d", seed, len(g.storage.storedIDs()), len(g.storage.appliedLogs()))
		}
		if len(g.storage.appliedLogs()) == len(g.storage.storedIDs()) {
			stable++
		} else {
			stable = 0
		}
		time.Sleep(20 * time.Millisecond)
	}

	applied := g.storage.appliedLogs()
	appliedAt := make(map[uint64]uint64, len(applied))
	appliedIDs := make(map[uint64]bool, len(applied))
	for i, log := range applied {
		require.Equal(t, uint64(i+1), log.Index, "seed %d: applied indexes are contiguous", seed)
		require.False(t, appliedIDs[log.Id], "seed %d: id %d applied twice", seed, log.Id)
		appliedIDs[log.Id] = true
		appliedAt[log.Index] = log.Id
	}
	stored := map[uint64]bool{}
	for _, id := range g.storage.storedIDs() {
		stored[id] = true
	}

	var ok, notAdmitted, unknown int
	for _, recs := range records {
		for _, rec := range recs {
			switch {
			case rec.err == nil:
				ok++
				require.Len(t, rec.resps, len(rec.ids))
				for j, resp := range rec.resps {
					require.Equal(t, rec.ids[j], resp.Id)
					require.Equal(t, resp.Id, appliedAt[resp.Index], "seed %d: success at index %d maps to another entry", seed, resp.Index)
				}
			case errors.Is(rec.err, types.ErrProposalNotAdmitted):
				notAdmitted++
				for _, id := range rec.ids {
					require.False(t, stored[id], "seed %d: not-admitted id %d entered the log", seed, id)
				}
			case errors.Is(rec.err, types.ErrProposalOutcomeUnknown):
				unknown++
			default:
				t.Fatalf("seed %d: unexpected outcome %v", seed, rec.err)
			}
		}
	}
	require.Zero(t, g.wait.pending(), "seed %d: receipts left", seed)
	require.Equal(t, 0, len(g.admissionSem(admKey)))
	cOK, cNotAdmitted, cUnknown, cSuperseded := g.ProposalOutcomeCounts()
	require.Equal(t, []uint64{uint64(ok), uint64(notAdmitted), uint64(unknown), 0},
		[]uint64{cOK, cNotAdmitted, cUnknown, cSuperseded})
	t.Logf("seed %d: proposals=%d ok=%d notAdmitted=%d unknown=%d applied=%d",
		seed, ok+notAdmitted+unknown, ok, notAdmitted, unknown, len(applied))
	require.NotZero(t, ok, "seed %d: vacuous run", seed)
	require.NotZero(t, notAdmitted, "seed %d: vacuous run", seed)
}

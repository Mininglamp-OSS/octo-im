package channel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

type confirmationReadDB struct {
	wkdb.DB
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newConfirmationReadDB(t *testing.T) *confirmationReadDB {
	t.Helper()
	return &confirmationReadDB{DB: retryDB(t), entered: make(chan struct{}), release: make(chan struct{})}
}

func (d *confirmationReadDB) LoadMsg(id string, typ uint8, index uint64) (wkdb.Message, error) {
	d.once.Do(func() {
		close(d.entered)
		<-d.release
	})
	return d.DB.LoadMsg(id, typ, index)
}

func TestPlannedTransferWaitsForMessageConfirmation(t *testing.T) {
	for _, name := range []string{"confirmed", "canceled"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			db := newConfirmationReadDB(t)
			releaseRead := sync.OnceFunc(func() { close(db.release) })
			transfer := make(chan types.Config, 1)
			network := retryNetwork()
			replicas := []uint64{1, 2, 3}
			old := retryServer(t, 1, db, replicas, network, WithOnSaveConfig(func(_ string, _ uint8, cfg types.Config) error {
				select {
				case transfer <- cfg:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}))
			targetDB := retryDB(t)
			next := retryServer(t, 2, targetDB, replicas, network)
			third := retryServer(t, 3, retryDB(t), replicas, network)

			type outcome struct {
				result types.ProposeRespSet
				err    error
			}
			result := make(chan outcome, 1)
			finished := make(chan struct{})
			proposalCtx, cancelProposal := context.WithCancel(ctx)
			reqs := retryRequests(t, retryMessage(100, "alice", "handoff"))
			go func() {
				defer close(finished)
				got, err := old.ProposeBatchUntilAppliedTimeout(proposalCtx, "retry", 2, reqs)
				result <- outcome{got, err}
			}()
			t.Cleanup(func() {
				cancelProposal()
				releaseRead()
				select {
				case <-finished:
				case <-time.After(2 * time.Second):
					t.Error("proposal did not exit after releasing the final read")
				}
			})

			select {
			case <-db.entered:
			case <-ctx.Done():
				t.Fatal("proposal did not reach its final canonical read")
			}
			applied, err := db.GetChannelAppliedIndex("retry", 2)
			require.NoError(t, err)
			require.Equal(t, uint64(1), applied)
			stored, err := db.DB.LoadMsg("retry", 2, applied)
			require.NoError(t, err)
			require.Equal(t, int64(100), stored.MessageID)
			require.Equal(t, uint32(1), stored.MessageSeq)
			require.Equal(t, []byte("payload"), stored.Payload)
			require.Eventually(t, func() bool {
				seq, _, err := targetDB.GetChannelLastMessageSeq("retry", 2)
				return err == nil && seq == 1
			}, 2*time.Second, 5*time.Millisecond)

			key := wkutil.ChannelToKey("retry", 2)
			var canConfirm bool
			require.NoError(t, old.getRaftGroup(key).Do(ctx, key, func(r raftgroup.IRaft) error {
				cfg := r.Config().Clone()
				cfg.Version++
				cfg.MigrateFrom, cfg.MigrateTo = 1, 2
				if err := r.Step(types.Event{Type: types.ConfChange, Config: cfg}); err != nil {
					return err
				}
				// Use the target's durable tail to enter the real transfer path
				// while the proposal caller is still blocked in the database.
				err := r.Step(types.Event{Type: types.SyncReq, Term: cfg.Term, From: 2,
					Index: 2, StoredIndex: 2, LastLogTerm: 1, Reason: types.ReasonOnlySync})
				canConfirm = r.(*Channel).LeaderReadReady()
				return err
			}))
			require.True(t, canConfirm, "the old leader closed confirmation before the pending result could be returned")
			enqueued := false
			require.ErrorIs(t, old.AdmitMessage(ctx, "retry", 2, 101, 64, func(func()) { enqueued = true }), ErrSendUnavailable)
			require.False(t, enqueued, "the old leader must already be draining admission")
			select {
			case cfg := <-transfer:
				t.Fatalf("transfer published before confirmation finished: %+v", cfg)
			default:
			}
			select {
			case got := <-result:
				t.Fatalf("proposal returned while the canonical read was blocked: %+v", got)
			default:
			}

			var cfg types.Config
			if name == "canceled" {
				cancelProposal()
				require.NoError(t, old.getRaftGroup(key).Do(ctx, key, func(r raftgroup.IRaft) error {
					// Cross the pre-publication drain deadline and cooldown without
					// releasing the canonical read. The canceled request must no
					// longer prevent a fresh durable report from triggering transfer.
					for i := 0; i < 220; i++ {
						r.Tick()
					}
					return r.Step(types.Event{Type: types.SyncReq, Term: r.Config().Term, From: 2,
						Index: 2, StoredIndex: 2, LastLogTerm: 1, Reason: types.ReasonOnlySync})
				}))
				select {
				case cfg = <-transfer:
				case <-time.After(2 * time.Second):
					t.Fatal("canceled request held the transfer while its canonical read was blocked")
				}
			}
			releaseRead()
			select {
			case got := <-result:
				if name == "canceled" {
					require.ErrorIs(t, got.err, context.Canceled)
					require.ErrorIs(t, got.err, ErrSendOutcomeUnknown)
					require.Empty(t, got.result)
				} else {
					require.NoError(t, got.err)
					require.Len(t, got.result, 1)
					require.Equal(t, uint64(100), got.result[0].CanonicalID)
					require.Equal(t, uint64(1), got.result[0].Index)
				}
			case <-ctx.Done():
				t.Fatal("proposal did not finish")
			}

			if name == "confirmed" {
				select {
				case cfg = <-transfer:
				case <-ctx.Done():
					t.Fatal("transfer did not resume after confirmation finished")
				}
			}
			require.Equal(t, uint64(2), cfg.Leader)
			require.Equal(t, uint32(2), cfg.Term)
			require.Zero(t, cfg.MigrateFrom)
			require.Zero(t, cfg.MigrateTo)

			// The test SaveConfig hook captured the chosen configuration.
			// Apply it with the version and local roles assigned by the authority.
			cfg.Version++
			cfg.Role = types.RoleFollower
			require.NoError(t, old.Channel("retry", 2).switchConfig(cfg))
			require.NoError(t, third.Channel("retry", 2).switchConfig(cfg))
			cfg.Role = types.RoleLeader
			require.NoError(t, next.Channel("retry", 2).switchConfig(cfg))
			res := retryPropose(t, next, retryMessage(102, "alice", "handoff"))
			require.Len(t, res, 1)
			require.Equal(t, uint64(100), res[0].CanonicalID)
			require.Equal(t, uint64(1), res[0].Index)
			require.True(t, res[0].Duplicate)
		})
	}
}

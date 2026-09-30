package cluster

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/stretchr/testify/require"
)

type applyTestNode struct{ icluster.Node }

func (applyTestNode) Slot(uint32) *nodetypes.Slot {
	return &nodetypes.Slot{Id: 0, Leader: 1, Term: 1, Replicas: []uint64{1}}
}
func (n applyTestNode) Slots() []*nodetypes.Slot { return []*nodetypes.Slot{n.Slot(0)} }

func TestSlotPermanentApplyFailureExitsRealWorkerProcess(t *testing.T) {
	if os.Getenv("PR54_APPLY_CRASH_CHILD") == "1" {
		s := &Server{store: store.New(store.NewOptions(store.WithDB(applyFailureDB{}))), opts: NewOptions(), Log: wklog.NewWKLog("cluster-test")}
		slots := slot.NewServer(slot.NewOptions(slot.WithNodeId(1), slot.WithDataDir(os.Getenv("PR54_APPLY_CRASH_DIR")), slot.WithSlotDbShardNum(1), slot.WithNode(applyTestNode{}), slot.WithApplyErrorRetry(true), slot.WithOnApply(s.slotApplyLogs), slot.WithOnSaveConfig(func(uint32, types.Config) error { return nil })))
		require.NoError(t, slots.Start())
		defer slots.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		slots.AddEvent("0", types.Event{Type: types.Propose, Logs: []types.Log{{Id: 1, Index: 1, Term: 1, Data: []byte{0}}}})
		<-ctx.Done()
		t.Fatal("permanent apply failure was swallowed by the worker pool")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSlotPermanentApplyFailureExitsRealWorkerProcess$")
	cmd.Env = append(os.Environ(), "PR54_APPLY_CRASH_CHILD=1", "PR54_APPLY_CRASH_DIR="+t.TempDir())
	out, err := cmd.CombinedOutput()
	require.Error(t, err, string(out))
	require.NoError(t, ctx.Err(), "poison apply must terminate promptly")
	require.Contains(t, string(out), "apply slot logs failed")
	require.Contains(t, string(out), "raftgroup:go pool panic", "the real slot pool re-panics instead of swallowing the failure")
	require.NotContains(t, string(out), "permanent apply failure was swallowed")
}

package cluster

import (
	"errors"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/stretchr/testify/require"
	"testing"
)

type applyFailureDB struct {
	wkdb.DB
	failure error
}

func (d applyFailureDB) SlotAppliedIndex(uint32) (uint64, error) { return 0, d.failure }

func TestSlotApplyLogsRetriesStorageErrorsEvenWhenRecoveryPaused(t *testing.T) {
	failure := errors.New("storage unavailable")
	for _, enabled := range []bool{true, false} {
		s := &Server{store: store.New(store.NewOptions(store.WithDB(applyFailureDB{failure: failure}))), opts: NewOptions(WithSubscriberRecoveryEnabled(enabled)), Log: wklog.NewWKLog("cluster-test")}
		require.NotPanics(t, func() { require.ErrorIs(t, s.slotApplyLogs(1, []rafttype.Log{{Index: 1}}), failure) })
	}
}

func TestSlotApplyLogsFailsFastOnMalformedCommittedEntry(t *testing.T) {
	s := &Server{store: store.New(store.NewOptions(store.WithDB(applyFailureDB{}))), opts: NewOptions(WithSubscriberRecoveryEnabled(true)), Log: wklog.NewWKLog("cluster-test")}
	require.Panics(t, func() { _ = s.slotApplyLogs(1, []rafttype.Log{{Index: 1, Data: []byte{0}}}) })
}

package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

// Use the ordinary slot proposal path on other channels in the same real slot.
// A metadata write must get an admission turn while this traffic keeps going.
func TestConfigSaveUnderContinuousSlotWrites(t *testing.T) {
	for _, writers := range []int{0, 1, 2, 4} {
		t.Run(fmt.Sprintf("writers=%d", writers), func(t *testing.T) {
			s, ctx := newConversationConsistencyServer(t, nil)
			s.configReconciler.stop()
			var completed atomic.Int64
			stop := make(chan struct{})
			errors := make(chan error, writers)
			var workers sync.WaitGroup
			for i := 0; i < writers; i++ {
				workers.Add(1)
				go func(i int) {
					defer workers.Done()
					cfg := consistencyConfig()
					cfg.ChannelId = fmt.Sprintf("ordinary-writer-%d", i)
					for {
						select {
						case <-stop:
							return
						default:
						}
						version, err := s.store.SaveChannelClusterConfig(cfg)
						if err != nil {
							errors <- err
							return
						}
						cfg.ConfVersion = version
						completed.Add(1)
					}
				}(i)
			}
			defer func() {
				close(stop)
				workers.Wait()
				close(errors)
				for err := range errors {
					require.NoError(t, err)
				}
			}()
			if writers > 0 {
				require.Eventually(t, func() bool { return completed.Load() >= 20 }, time.Second, time.Millisecond)
			}
			startCount := completed.Load()
			for attempt := 0; attempt < 4; attempt++ {
				cfg := consistencyConfig()
				cfg.ChannelId = fmt.Sprintf("fenced-write-%d", attempt)
				cfg.ConfVersion = 0
				writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				started := time.Now()
				version, err := s.saveChannelConfig(writeCtx, cfg)
				cancel()
				t.Logf("writers=%d attempt=%d elapsed=%s background_writes=%d", writers, attempt, time.Since(started), completed.Load()-startCount)
				require.NoError(t, err, "ordinary slot writes must not starve channel metadata")
				current, err := s.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
				require.NoError(t, err)
				require.Equal(t, version, current.ConfVersion)
				require.Equal(t, cfg.Term, current.Term)
				updated := current.Clone()
				updated.Term++
				version, err = s.saveChannelConfig(ctx, updated)
				require.NoError(t, err)
				_, err = s.saveChannelConfig(ctx, current)
				require.ErrorIs(t, err, raft.ErrConfigVersionStale, "busy-slot admission must retain the version fence")
				winner, err := s.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
				require.NoError(t, err)
				require.Equal(t, version, winner.ConfVersion)
				require.Equal(t, updated.Term, winner.Term)
			}
			// Exercise both user-visible creation/first send and the production
			// maintenance path while the same ordinary writers are still active.
			msg := consistencyMessage(1)
			msg.ChannelID, msg.MessageSeq = "first-send-under-load", 0
			sendCtx, cancelSend := context.WithTimeout(ctx, 5*time.Second)
			resps, err := s.store.AppendMessages(sendCtx, msg.ChannelID, msg.ChannelType, []wkdb.Message{msg})
			cancelSend()
			require.NoError(t, err)
			require.Len(t, resps, 1)
			require.Equal(t, uint64(1), resps[0].Index)
			messages, err := s.db.LoadLastMsgs(msg.ChannelID, msg.ChannelType, 10)
			require.NoError(t, err)
			require.Len(t, messages, 1)
			require.Equal(t, msg.MessageID, messages[0].MessageID)
			require.Equal(t, msg.Payload, messages[0].Payload)
			leaderless := consistencyConfig()
			leaderless.ChannelId, leaderless.LeaderId, leaderless.ConfVersion = "recovery-under-load", 0, 0
			_, err = s.saveChannelConfig(ctx, leaderless)
			require.NoError(t, err)
			recoverCtx, cancelRecovery := context.WithTimeout(ctx, 2*time.Second)
			err = s.reconcileChannelConfig(recoverCtx, channelConfigKey{leaderless.ChannelId, leaderless.ChannelType})
			cancelRecovery()
			require.NoError(t, err)
			recovered, err := s.loadConversationConfig(ctx, leaderless.ChannelId, leaderless.ChannelType)
			require.NoError(t, err)
			require.Equal(t, uint64(1), recovered.LeaderId)
			require.Greater(t, recovered.Term, leaderless.Term)
			if writers > 0 {
				require.Greater(t, completed.Load(), startCount, "ordinary traffic must also keep progressing")
			}
		})
	}
}

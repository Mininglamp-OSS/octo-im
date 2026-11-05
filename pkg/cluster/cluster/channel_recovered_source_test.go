package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// Drive the admitted C->D task through the real heartbeat/join/rejoin paths.
// B's channel traffic can fail independently of the configuration and slot
// groups, so the metadata owner remains available throughout the transition.
func TestChannelRecoveredSourceSurvivesAnotherFailure(t *testing.T) {
	for _, stopNode := range []bool{false, true} {
		name := "channel-unreachable"
		if stopNode {
			name = "node-crashed"
		}
		t.Run(name, func(t *testing.T) { recoveredSourceSurvivesAnotherFailure(t, stopNode) })
	}
}

func recoveredSourceSurvivesAnotherFailure(t *testing.T, stopNode bool) {
	f := newExpansionFixture(t)
	const source = uint64(3)
	var channel string
	var leader uint64
	for _, id := range f.channelIDs {
		cfg, err := f.servers[1].loadConversationConfig(f.ctx, id, 2)
		require.NoError(t, err)
		if cfg.LeaderId != source {
			channel, leader = id, cfg.LeaderId
			break
		}
	}
	require.NotEmpty(t, channel, "need a follower source")
	other := uint64(1)
	if leader == other {
		other = 2
	}
	f.stop(source)
	f.waitOffline(source)
	seed := f.live()
	f.start(4, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, true)
	f.slots[4].pause.Store(false)
	var planned wkdb.ChannelClusterConfig
	require.Eventually(t, func() bool {
		var err error
		planned, err = f.servers[leader].loadConversationConfig(f.ctx, channel, 2)
		return err == nil && planned.MigrateFrom == source && planned.MigrateTo == 4 &&
			wkutil.ArrayContainsUint64(planned.Learners, 4)
	}, 10*time.Second, 25*time.Millisecond, "normal expansion must persist C->D before C returns")
	f.start(source, "", false, false)
	require.Eventually(t, func() bool {
		for _, s := range f.servers {
			cfg := s.cfgServer.Snapshot()
			if len(cfg.Nodes) != 4 || len(cfg.Learners) != 0 {
				return false
			}
			for _, n := range cfg.Nodes {
				if !n.Online || n.Status != nodetypes.NodeStatus_NodeStatusJoined {
					return false
				}
			}
		}
		state, err := f.servers[source].channelServer.ReadLeaderState(f.ctx, channel, 2)
		return err == nil && state.Exists && state.LeaderID == leader && state.ConfigVersion >= planned.ConfVersion
	}, 15*time.Second, 25*time.Millisecond, "C must rejoin and reinstall the admitted channel before B fails")
	if stopNode {
		f.stop(other)
		f.waitOffline(other)
	} else {
		f.channels[other].pause.Store(true)
		t.Cleanup(func() { f.channels[other].pause.Store(false) })
	}
	appendMessage := func(seq int) error {
		ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
		defer cancel()
		_, err := f.servers[leader].store.AppendMessages(ctx, channel, 2, []wkdb.Message{{RecvPacket: wkproto.RecvPacket{
			ChannelID: channel, ChannelType: 2, FromUID: "expansion-test", MessageID: int64(seq),
			ClientMsgNo: fmt.Sprintf("%s-%d", channel, seq), Payload: []byte(fmt.Sprintf("payload-%s-%d", channel, seq)),
		}}})
		return err
	}
	require.NoError(t, appendMessage(5), "A+C must still commit while B cannot synchronize and D is paused")
	t.Logf("before completion: voters=%v learners=%v migration=%d->%d leader=%d; B=%d unavailable; write 5 committed",
		planned.Replicas, planned.Learners, planned.MigrateFrom, planned.MigrateTo, leader, other)
	f.channels[4].pause.Store(false)
	var after wkdb.ChannelClusterConfig
	require.Eventually(t, func() bool {
		var err error
		after, err = f.servers[leader].loadConversationConfig(f.ctx, channel, 2)
		return err == nil && after.ConfVersion > planned.ConfVersion &&
			(wkutil.ArrayContainsUint64(after.Replicas, 4) || !wkutil.ArrayContainsUint64(after.Replicas, source))
	}, 8*time.Second, 25*time.Millisecond)
	t.Logf("after completion starts: voters=%v learners=%v migration=%d->%d", after.Replicas, after.Learners, after.MigrateFrom, after.MigrateTo)
	require.NoError(t, appendMessage(6), "D's completion must preserve writes without B returning")
	require.Eventually(t, func() bool {
		cfg, err := f.servers[leader].loadConversationConfig(f.ctx, channel, 2)
		retired := source
		if stopNode {
			retired = other // maintenance heals B after the admitted C->D task
		}
		return err == nil && len(cfg.Replicas) == 3 && len(cfg.Learners) == 0 && cfg.MigrateFrom == 0 && cfg.MigrateTo == 0 &&
			wkutil.ArrayContainsUint64(cfg.Replicas, 4) && !wkutil.ArrayContainsUint64(cfg.Replicas, retired)
	}, 8*time.Second, 25*time.Millisecond, "the admitted migration must finish with B still unavailable")
	verify := []uint64{leader, 4}
	if stopNode {
		verify = append(verify, source)
	}
	for _, id := range verify {
		require.Eventually(t, func() bool {
			messages, err := f.servers[id].db.LoadLastMsgs(channel, 2, 20)
			if err != nil || len(messages) != 6 {
				return false
			}
			for i, message := range messages {
				if message.MessageID != int64(i+1) || message.MessageSeq != uint32(i+1) || string(message.Payload) != fmt.Sprintf("payload-%s-%d", channel, i+1) {
					return false
				}
			}
			return true
		}, 5*time.Second, 25*time.Millisecond, "node %d must retain all history and both committed writes", id)
	}
	seq, err := f.servers[leader].GetChannelLastMessageSeq(f.ctx, channel, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(6), seq, "the public read fence must observe the committed post-transition write")
}

// Hold D's acknowledgement of the four-voter configuration so each interruption
// happens after durable admission but before the source can be retired.
func TestChannelFollowerMigrationResumesAddFirstStage(t *testing.T) {
	for _, interruption := range []string{"destination-restart", "metadata-owner-restart", "message-leader-failure", "destination-crash"} {
		t.Run(interruption, func(t *testing.T) {
			f := newExpansionFixture(t)
			defer func() {
				if t.Failed() {
					f.dump()
				}
			}()
			seed := fmt.Sprintf("1@%s", f.addresses[1])
			f.start(4, seed, false, false)
			f.waitStable([]uint64{1, 2, 3, 4}, 4)
			var cfg wkdb.ChannelClusterConfig
			var owner uint64
			for _, id := range f.channelIDs {
				owner = f.servers[1].cfgServer.SlotLeaderId(f.servers[1].getSlotId(id))
				if owner != 4 {
					var err error
					cfg, err = f.servers[owner].loadConversationConfig(f.ctx, id, 2)
					require.NoError(t, err)
					break
				}
			}
			require.NotEmpty(t, cfg.ChannelId)
			require.NotContains(t, cfg.Replicas, uint64(4), "healthy expansion preserves already-full message groups")
			source := uint64(1)
			if cfg.LeaderId == source {
				source = 2
			}
			f.channels[4].pause.Store(true)
			cfg = cfg.Clone()
			cfg.Learners = []uint64{4}
			cfg.MigrateFrom, cfg.MigrateTo = source, 4
			version, err := f.servers[owner].saveChannelConfig(f.ctx, cfg)
			require.NoError(t, err)
			f.channels[4].maxConfigVersion.Store(version)
			f.channels[4].pause.Store(false)
			t.Cleanup(func() { f.channels[4].maxConfigVersion.Store(0) })
			var stage wkdb.ChannelClusterConfig
			require.Eventually(t, func() bool {
				var err error
				stage, err = f.servers[owner].loadConversationConfig(f.ctx, cfg.ChannelId, 2)
				return err == nil && len(stage.Replicas) == 4 && len(stage.Learners) == 0 &&
					stage.MigrateFrom == source && stage.MigrateTo == 4
			}, 10*time.Second, 25*time.Millisecond, "D must be admitted before the recovered source retires")
			t.Logf("persisted intermediate stage before %s: voters=%v learners=%v migration=%d->%d version=%d leader=%d owner=%d",
				interruption, stage.Replicas, stage.Learners, stage.MigrateFrom, stage.MigrateTo, stage.ConfVersion, stage.LeaderId, owner)
			appendMessage := func(seq int) error {
				ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
				defer cancel()
				_, err := f.servers[f.live()].store.AppendMessages(ctx, cfg.ChannelId, 2, []wkdb.Message{{RecvPacket: wkproto.RecvPacket{
					ChannelID: cfg.ChannelId, ChannelType: 2, FromUID: "expansion-test", MessageID: int64(seq),
					ClientMsgNo: fmt.Sprintf("%s-%d", cfg.ChannelId, seq), Payload: []byte(fmt.Sprintf("payload-%s-%d", cfg.ChannelId, seq)),
				}}})
				return err
			}
			require.NoError(t, appendMessage(5), "the old majority must keep serving during intermediate installation")
			switch interruption {
			case "destination-restart":
				f.stop(4)
				f.start(4, seed, false, false)
			case "metadata-owner-restart":
				f.stop(owner)
				f.start(owner, "", false, false)
				require.Eventually(t, func() bool {
					current, err := f.servers[owner].loadConversationConfig(f.ctx, cfg.ChannelId, 2)
					return err == nil && len(current.Replicas) == 4 && current.MigrateFrom != 0 && current.MigrateTo == 4
				}, 10*time.Second, 25*time.Millisecond, "the intermediate task must survive controller replay")
				f.channels[4].maxConfigVersion.Store(0)
			case "message-leader-failure":
				f.stop(cfg.LeaderId)
				f.waitOffline(cfg.LeaderId)
				require.Eventually(t, func() bool {
					current, err := f.servers[f.live()].loadConversationConfig(f.ctx, cfg.ChannelId, 2)
					return err == nil && current.LeaderId == source && current.MigrateFrom == cfg.LeaderId && current.MigrateTo == 4
				}, 10*time.Second, 25*time.Millisecond, "the recovered source must lead while the failed old leader is retired")
				f.channels[4].maxConfigVersion.Store(0)
			case "destination-crash":
				f.stop(4)
				f.waitOffline(4)
			}
			var final wkdb.ChannelClusterConfig
			resumed := time.Now()
			require.Eventually(t, func() bool {
				var err error
				final, err = f.servers[f.live()].loadConversationConfig(f.ctx, cfg.ChannelId, 2)
				if err != nil || len(final.Replicas) != 3 || len(final.Learners) != 0 || final.MigrateFrom != 0 || final.MigrateTo != 0 {
					return false
				}
				if interruption == "destination-crash" {
					return !wkutil.ArrayContainsUint64(final.Replicas, 4) && wkutil.ArrayContainsUint64(final.Replicas, source)
				}
				return wkutil.ArrayContainsUint64(final.Replicas, 4) && wkutil.ArrayContainsUint64(final.Replicas, final.LeaderId)
			}, 45*time.Second, 25*time.Millisecond, "resume or cancel within the existing 30-second background scan plus catch-up")
			t.Logf("after %s: voters=%v leader=%d migration=%d->%d resumed in %s", interruption,
				final.Replicas, final.LeaderId, final.MigrateFrom, final.MigrateTo, time.Since(resumed))
			require.NoError(t, appendMessage(6))
			for _, id := range final.Replicas {
				require.Eventually(t, func() bool {
					messages, err := f.servers[id].db.LoadLastMsgs(cfg.ChannelId, 2, 20)
					if err != nil || len(messages) != 6 {
						return false
					}
					for i, message := range messages {
						if message.MessageID != int64(i+1) || message.MessageSeq != uint32(i+1) || string(message.Payload) != fmt.Sprintf("payload-%s-%d", cfg.ChannelId, i+1) {
							return false
						}
					}
					return true
				}, 5*time.Second, 25*time.Millisecond, "node %d must persist all original and post-interruption messages", id)
			}
		})
	}
}

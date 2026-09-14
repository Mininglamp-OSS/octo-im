package channel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

var ErrMessageConflict = errors.New("client_msg_no already used for different message content")
var errMessageLeaderChanged = errors.New("channel leader changed; retry message")
var errMessageSnapshotChanged = errors.New("channel log changed during lookup")
var errMessageNotReady = errors.New("channel leader is preparing durable state")

type messageRetryKey struct{ sender, client string }

// proposeMessages snapshots buffered entries on the owner, reads the database
// off-owner, then revalidates the snapshot before allocating indices. Store
// completion may remove buffered entries while we read: the snapshot already
// includes them. Appends/replacements/truncations invalidate the snapshot.
// Stored messages are Raft log entries, not proof of commitment.
func (s *Server) proposeMessages(ctx context.Context, id string, typ uint8, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	key := wkutil.ChannelToKey(id, typ)
	rg := s.getRaftGroup(key)
	messages := make([]wkdb.Message, len(reqs))
	for i, req := range reqs {
		if err := messages[i].Unmarshal(req.Data); err != nil {
			return nil, err
		}
		m := messages[i]
		if m.ChannelID != id || m.ChannelType != typ || m.MessageID <= 0 || uint64(m.MessageID) != req.Id {
			return nil, errors.New("invalid channel message proposal")
		}
	}
	var owner *Channel
	var term uint32
	var version uint64
	var maxIndex uint64
	resps := make(types.ProposeRespSet, len(reqs))
	admit := func() error {
		var revision uint64
		pending := make(map[messageRetryKey]wkdb.Message)
		err := rg.Do(ctx, key, func(r raftgroup.IRaft) error {
			ch, ok := r.(*Channel)
			if !ok || !ch.IsLeader() {
				return errMessageLeaderChanged
			}
			if !ch.LeaderReadReady() {
				return errMessageNotReady
			}
			owner, term, version = ch, ch.Config().Term, ch.Config().Version
			revision = ch.LogRevision()
			for _, log := range ch.BufferedLogs() {
				var m wkdb.Message
				if err := m.Unmarshal(log.Data); err != nil {
					return err
				}
				m.MessageSeq = uint32(log.Index)
				if m.ClientMsgNo != "" {
					m.Payload = bytes.Clone(m.Payload) // snapshot must not retain Raft-owned bytes
					pending[messageRetryKey{m.FromUID, m.ClientMsgNo}] = m
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		// No database operation is allowed in a RaftGroup.Do callback.
		stored := make(map[messageRetryKey]wkdb.Message)
		lookedUp := make(map[messageRetryKey]bool)
		for _, m := range messages {
			if err := ctx.Err(); err != nil {
				return err
			}
			k := messageRetryKey{m.FromUID, m.ClientMsgNo}
			if m.ClientMsgNo == "" || lookedUp[k] {
				continue
			}
			lookedUp[k] = true
			if _, ok := pending[k]; ok {
				continue
			}
			old, err := s.opts.DB.LoadMsgBySenderClientMsgNo(id, typ, m.FromUID, m.ClientMsgNo)
			if err != nil && !errors.Is(err, wkdb.ErrNotFound) {
				return err
			}
			if err == nil {
				stored[k] = old
			}
		}
		return rg.Do(ctx, key, func(r raftgroup.IRaft) error {
			ch, ok := r.(*Channel)
			if !ok || ch != owner || !ch.IsLeader() || ch.Config().Term != term {
				return errMessageLeaderChanged
			}
			if !ch.LeaderReadReady() {
				return errMessageNotReady
			}
			if ch.Config().Version != version || ch.LogRevision() != revision {
				return errMessageSnapshotChanged
			}
			maxIndex = 0
			next := ch.LastLogIndex()
			logs := make([]types.Log, 0, len(reqs))
			for i, m := range messages {
				canonical := m
				duplicate := false
				k := messageRetryKey{m.FromUID, m.ClientMsgNo}
				if m.ClientMsgNo != "" {
					old, found := pending[k]
					if !found {
						old, found = stored[k]
					}
					if found {
						if old.MessageSeq == 0 || uint64(old.MessageSeq) > next {
							return errors.New("retry entry outside channel log")
						}
						if !sameMessageContent(old, m) {
							return ErrMessageConflict
						}
						canonical, duplicate = old, true
					}
				}
				if !duplicate {
					next++
					if next > math.MaxUint32 {
						return errors.New("channel message sequence exhausted")
					}
					canonical.MessageSeq = uint32(next)
					logs = append(logs, types.Log{Id: reqs[i].Id, Term: term, Index: next, Data: reqs[i].Data})
					if m.ClientMsgNo != "" {
						pending[k] = canonical
					}
				}
				resps[i] = &types.ProposeResp{Id: reqs[i].Id, Index: uint64(canonical.MessageSeq), CanonicalID: uint64(canonical.MessageID), Duplicate: duplicate}
				maxIndex = max(maxIndex, uint64(canonical.MessageSeq))
			}
			// A conflicting input rejects the entire batch before assigning any index.
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(logs) > 0 {
				if err := ch.Step(types.Event{Type: types.Propose, Logs: logs}); err != nil {
					return err
				}
			}
			ch.ResumeReplication()
			return nil
		})
	}
	var err error
	for {
		err = admit()
		if !errors.Is(err, errMessageNotReady) && !errors.Is(err, errMessageSnapshotChanged) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err != nil {
		return nil, err
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		done := false
		err = rg.Do(ctx, key, func(r raftgroup.IRaft) error {
			ch, ok := r.(*Channel)
			if !ok || ch != owner || !ch.IsLeader() || ch.Config().Term != term {
				return errMessageLeaderChanged
			}
			ch.KeepAlive()
			// Initial durable-state persistence and a membership update can temporarily
			// fence the same leader. No new index is allocated while waiting. Observe
			// the new configuration before checking its committed/applied boundary.
			if !ch.LeaderReadReady() {
				return nil
			}
			if ch.Config().Version != version {
				version = ch.Config().Version
				return nil
			}
			if ch.CommittedIndex() < maxIndex || ch.AppliedIndex() < maxIndex {
				return nil
			}

			done = true
			return nil
		})
		if err != nil {
			return nil, err
		}
		if done {
			// The committed/applied prefix is immutable. Verify the canonical
			// payload off-owner, then certify that this same leader/config is
			// still serving that prefix. A new append does not invalidate it.
			for i, resp := range resps {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				stored, err := s.opts.DB.LoadMsg(id, typ, resp.Index)
				if err != nil {
					return nil, err
				}
				if uint64(stored.MessageID) != resp.CanonicalID || !sameMessageContent(stored, messages[i]) {
					return nil, fmt.Errorf("canonical message changed at index %d", resp.Index)
				}
			}
			err = rg.Do(ctx, key, func(r raftgroup.IRaft) error {
				ch, ok := r.(*Channel)
				if !ok || ch != owner || !ch.IsLeader() || ch.Config().Term != term {
					return errMessageLeaderChanged
				}
				if !ch.LeaderReadReady() || ch.Config().Version != version || ch.CommittedIndex() < maxIndex || ch.AppliedIndex() < maxIndex {
					return errMessageNotReady
				}
				return nil
			})
			if err == nil {
				return resps, nil
			}
			if !errors.Is(err, errMessageNotReady) {
				return nil, err
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// Exclude server identity, timestamps, transport client sequence and DUP. They
// can change on retries. Include every persisted immutable content field.
func sameMessageContent(a, b wkdb.Message) bool {
	return a.FromUID == b.FromUID && a.ChannelID == b.ChannelID && a.ChannelType == b.ChannelType &&
		a.ClientMsgNo == b.ClientMsgNo && a.Setting == b.Setting && a.Expire == b.Expire &&
		a.Topic == b.Topic && a.StreamNo == b.StreamNo && a.StreamId == b.StreamId && a.StreamFlag == b.StreamFlag &&
		a.NoPersist == b.NoPersist && a.RedDot == b.RedDot && a.SyncOnce == b.SyncOnce && bytes.Equal(a.Payload, b.Payload)
}

// ValidateMessageResults rejects peers that cannot return canonical identity.
func ValidateMessageResults(reqs types.ProposeReqSet, resps types.ProposeRespSet) error {
	if len(reqs) != len(resps) {
		return fmt.Errorf("incomplete channel proposal response")
	}
	for i, r := range resps {
		if r == nil || r.Id != reqs[i].Id || r.Index == 0 || r.Index > math.MaxUint32 || r.CanonicalID == 0 {
			return fmt.Errorf("invalid channel proposal response")
		}
	}
	return nil
}

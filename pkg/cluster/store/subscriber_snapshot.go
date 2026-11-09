package store

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// Subscriber revisions identify cache snapshots, not replicated database
// versions. Every new Store/channel state starts with a fresh random token, so
// remote caches cannot mistake a restarted/new owner for an earlier snapshot.
// Membership remains durable in the slot database; no retry queue is required
// to recover a lost Tag invalidation.
type subscriberState struct {
	sync.RWMutex
	revision string
}

type SubscriberSnapshot struct {
	Revision string
	Changed  bool
	Members  []wkdb.Member
}

func (s *Store) subscriberState(id string, typ uint8) *subscriberState {
	key := wkutil.ChannelToKey(id, typ)
	if v, ok := s.subscriberStates.Load(key); ok {
		return v.(*subscriberState)
	}
	v, _ := s.subscriberStates.LoadOrStore(key, &subscriberState{revision: wkutil.GenUUID()})
	return v.(*subscriberState)
}

// GetSubscriberSnapshot reads membership and its token under the same lock as
// slot application. A matching token avoids reading/returning the member list.
func (s *Store) GetSubscriberSnapshot(id string, typ uint8, known string) (SubscriberSnapshot, error) {
	state := s.subscriberState(id, typ)
	state.RLock()
	defer state.RUnlock()
	result := SubscriberSnapshot{Revision: state.revision, Changed: known != state.revision}
	if !result.Changed {
		return result, nil
	}
	var err error
	result.Members, err = s.wdb.GetSubscribers(id, typ)
	return result, err
}

func (s *Store) changeSubscribers(id string, typ uint8, apply func() error) error {
	state := s.subscriberState(id, typ)
	state.Lock()
	defer state.Unlock()
	err := apply()
	// Invalidate even on a storage error: a failed write must never certify a
	// potentially stale cache. The next successful snapshot reads actual data.
	state.revision = wkutil.GenUUID()
	return err
}

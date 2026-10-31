package event

import (
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
)

// OnlineUserCount counts authenticated UIDs whose slot is currently led here.
// It includes recovered sessions without requiring a user event, and excludes
// cached handlers on previous leaders. Multiple devices count as one UID.
//
// Collections may run concurrently with each other and with session updates.
// Each collection caches one leader decision per slot. Cluster-wide samples
// converge after leader propagation and presence recovery; they are not a
// distributed atomic snapshot during a handoff.
func (e *EventPool) OnlineUserCount() int64 {
	cluster := service.Cluster
	localNodeID := options.G.Cluster.NodeId
	if cluster == nil || localNodeID == 0 {
		return 0
	}
	ownedSlots := make(map[uint32]bool)
	var count int64
	for _, p := range e.pollers {
		p.waitlist.mu.RLock()
		for node := p.waitlist.head; node != nil; node = node.next {
			h := node.handler
			slotID := cluster.GetSlotId(h.Uid)
			owned, known := ownedSlots[slotID]
			if !known {
				owned = cluster.SlotLeaderId(slotID) == localNodeID
				ownedSlots[slotID] = owned
			}
			if owned && h.conns.hasAuthedConn() {
				count++
			}
		}
		p.waitlist.mu.RUnlock()
	}
	return count
}

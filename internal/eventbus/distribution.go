package eventbus

import (
	"time"

	"github.com/WuKongIM/WuKongIM/internal/types"
)

// DistributionState belongs to one channel worker and survives local retries.
// Snapshot is immutable and is not part of the legacy event encoding.
type DistributionState struct {
	Snapshot       *types.Tag          // Snapshot fixes this message's recipient version.
	LocalDone      bool                // LocalDone prevents replay of local fan-out.
	DeliveredNodes map[uint64]struct{} // DeliveredNodes records acknowledged remote admissions.
	Attempts       int                 // Attempts bounds retry work for this message.
	Deadline       time.Time           // Deadline uses the local monotonic clock.
}

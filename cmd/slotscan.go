package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	"github.com/spf13/cobra"
)

// slotScanCMD is the pre-upgrade divergence scan. Run it on every node with
// the node stopped; the upgrade must not proceed while any slot would open
// quarantined on a quorum of its replicas.
type slotScanCMD struct {
	ctx       *WuKongIMContext
	dataDir   string
	slotCount uint32
	shards    uint32
}

func newSlotScanCMD(ctx *WuKongIMContext) *slotScanCMD {
	return &slotScanCMD{ctx: ctx}
}

func (s *slotScanCMD) CMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "slot-scan",
		Short: "read-only scan of a stopped node for slot replicas that would open quarantined",
		RunE:  s.run,
	}
	cmd.Flags().StringVar(&s.dataDir, "dataDir", "", "node data directory (defaults to the configured dataDir)")
	cmd.Flags().Uint32Var(&s.slotCount, "slotCount", 0, "slot count (defaults to the configured cluster.slotCount)")
	cmd.Flags().Uint32Var(&s.shards, "shards", 8, "slot log storage shard count")
	return cmd
}

func (s *slotScanCMD) run(cmd *cobra.Command, args []string) error {
	dataDir := s.dataDir
	if dataDir == "" {
		dataDir = serverOpts.DataDir
	}
	slotCount := s.slotCount
	if slotCount == 0 {
		slotCount = uint32(serverOpts.Cluster.SlotCount)
	}
	logDB := slot.SlotLogDBPath(path.Join(dataDir, "cluster"))
	flagged, err := slot.ScanDivergence(logDB, s.shards, slotCount)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	for _, r := range flagged {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "slot-scan: %s: %d of %d slots would open quarantined\n", logDB, len(flagged), slotCount)
	if len(flagged) > 0 {
		return fmt.Errorf("slot-scan: %d slot replicas would open quarantined; resolve them before upgrading", len(flagged))
	}
	return nil
}

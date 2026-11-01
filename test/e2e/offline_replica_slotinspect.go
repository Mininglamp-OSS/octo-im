//go:build ignore

package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"strconv"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot/key"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/cockroachdb/pebble"
)

func main() {
	shards, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	dbs := make([]*pebble.DB, shards)
	for i := range dbs {
		dbs[i], err = pebble.Open(fmt.Sprintf("%s/shard%03d", os.Args[1], i), &pebble.Options{ReadOnly: true})
		if err != nil {
			panic(err)
		}
		defer dbs[i].Close()
	}
	out := map[string]any{}
	for slot := 0; slot < 64; slot++ {
		sn := strconv.Itoa(slot)
		h := fnv.New32()
		h.Write([]byte(sn))
		db := dbs[int(h.Sum32())%shards]
		applied := uint64(0)
		v, closer, err := db.Get(key.NewAppliedIndexKey(sn))
		if err == nil {
			applied = binary.BigEndian.Uint64(v)
			closer.Close()
		}
		iter := db.NewIter(&pebble.IterOptions{LowerBound: key.NewLogKey(sn, 0), UpperBound: key.NewLogKey(sn, math.MaxUint64)})
		logs := []any{}
		for iter.First(); iter.Valid(); iter.Next() {
			v := iter.Value()
			var log types.Log
			if err := log.Unmarshal(v[:len(v)-8]); err != nil {
				panic(err)
			}
			cmd := &store.CMD{}
			if err := cmd.Unmarshal(log.Data); err != nil {
				panic(err)
			}
			row := map[string]any{"index": log.Index, "term": log.Term, "id": log.Id, "cmd": cmd.CmdType.String(), "data_sha256": fmt.Sprintf("%x", sha256.Sum256(log.Data))}
			if cmd.CmdType == store.CMDChannelClusterConfigSave {
				_, _, data, err := cmd.DecodeCMDChannelClusterConfigSave()
				if err != nil {
					panic(err)
				}
				var cfg wkdb.ChannelClusterConfig
				if err := cfg.Unmarshal(data); err != nil {
					panic(err)
				}
				row["config"] = cfg
			}
			logs = append(logs, row)
		}
		if err := iter.Error(); err != nil {
			panic(err)
		}
		iter.Close()
		out[sn] = map[string]any{"applied": applied, "logs": logs}
	}
	b, err := json.Marshal(out)
	if err != nil {
		panic(err)
	}
	fmt.Println("REPLACEMENT_SLOT_JSON=" + string(b))
}

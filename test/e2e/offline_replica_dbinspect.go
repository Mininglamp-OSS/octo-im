//go:build ignore

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

func main() {
	if len(os.Args) != 3 {
		panic("usage: dbinspect <db-dir> <shards>; JSON channel list on stdin")
	}
	shards, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	var channels []string
	if err = json.NewDecoder(os.Stdin).Decode(&channels); err != nil {
		panic(err)
	}
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(os.Args[1]), wkdb.WithShardNum(shards), wkdb.WithNodeId(1001), wkdb.WithSlotCount(64)))
	if err = db.Open(); err != nil {
		panic(err)
	}
	out := map[string]interface{}{}
	for _, c := range channels {
		cfg, ce := db.GetChannelClusterConfig(c, 2)
		msgs, me := db.LoadLastMsgs(c, 2, 1000)
		out[c] = map[string]interface{}{"config": cfg, "config_error": fmt.Sprint(ce), "messages": msgs, "messages_error": fmt.Sprint(me)}
	}
	if err = db.Close(); err != nil {
		panic(err)
	}
	b, err := json.Marshal(out)
	if err != nil {
		panic(err)
	}
	fmt.Println("REPLACEMENT_DB_JSON=" + string(b))
}

//go:build ignore

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/client"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// This driver uses the repository's real encrypted TCP client and records
// SENDACK separately from submission; no HTTP response is treated as an ACK.
func main() {
	var mu sync.Mutex
	out := json.NewEncoder(os.Stdout)
	emit := func(v any) { mu.Lock(); defer mu.Unlock(); _ = out.Encode(v) }
	c := client.New(os.Args[1], client.WithUID(os.Args[2]))
	acks := make(chan *wkproto.SendackPacket, 1024)
	c.SetOnSendack(func(a *wkproto.SendackPacket) { acks <- a })
	c.SetOnRecv(func(r *wkproto.RecvPacket) error {
		emit(map[string]any{"event": "recv", "channel": r.ChannelID, "id": r.MessageID, "seq": r.MessageSeq, "key": r.ClientMsgNo, "payload": string(r.Payload)})
		return nil
	})
	if err := c.Connect(); err != nil {
		emit(map[string]any{"event": "connect", "error": err.Error()})
		os.Exit(1)
	}
	defer c.Close()
	emit(map[string]any{"event": "connect", "ok": true})
	sc := bufio.NewScanner(os.Stdin)
	var seq uint64
	for sc.Scan() {
		var r struct {
			Channel string `json:"channel"`
			Key     string `json:"key"`
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			panic(err)
		}
		if err := c.SendMessage(&client.Channel{ChannelID: r.Channel, ChannelType: 2}, []byte(r.Payload), func(o *client.SendOptions) error { o.ClientMsgNo = r.Key; return nil }); err != nil {
			emit(map[string]any{"event": "send", "key": r.Key, "error": err.Error()})
			continue
		}
		seq++
		timer := time.NewTimer(12 * time.Second)
	wait:
		for {
			select {
			case a := <-acks:
				if a.ClientSeq == seq {
					emit(map[string]any{"event": "ack", "channel": r.Channel, "key": r.Key, "client_seq": a.ClientSeq, "id": a.MessageID, "seq": a.MessageSeq, "reason": a.ReasonCode, "payload": r.Payload})
					timer.Stop()
					break wait
				}
				emit(map[string]any{"event": "late_ack", "client_seq": a.ClientSeq, "id": a.MessageID, "seq": a.MessageSeq, "reason": a.ReasonCode})
			case <-timer.C:
				emit(map[string]any{"event": "timeout", "key": r.Key})
				break wait
			}
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

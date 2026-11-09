package tagreset_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/pkg/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// This transport proxy only rejects the selected Tag mutation RPC. Consensus,
// snapshot reads, message sends and authenticated TCP sessions keep running.
// No fault-injection hook is added to the production server.
type tagRPCFaults struct {
	sync.Mutex
	channel  string
	rejected int
}

func (f *tagRPCFaults) reject(typ proto.MsgType, body []byte) []byte {
	if typ != proto.MsgTypeRequest {
		return nil
	}
	var req proto.Request
	if req.Unmarshal(body) != nil {
		return nil
	}
	var id string
	switch req.Path {
	case "/wk/ingress/updateTag":
		var r ingress.TagUpdateReq
		if r.Decode(req.Body) != nil {
			return nil
		}
		id = r.ChannelId
	case "/wk/ingress/invalidateChannelTag":
		var r ingress.ChannelReq
		if r.Decode(req.Body) != nil {
			return nil
		}
		id = r.ChannelId
	default:
		return nil
	}
	f.Lock()
	defer f.Unlock()
	if f.channel == "" || id != f.channel {
		return nil
	}
	f.rejected++
	response, _ := (&proto.Response{Id: req.Id, Status: proto.StatusError, Body: []byte("injected second Tag owner failure")}).Marshal()
	frame, _ := proto.New().Encode(response, proto.MsgTypeResp)
	return frame
}

type serializedConn struct {
	net.Conn
	sync.Mutex
}

func (c *serializedConn) Write(b []byte) (int, error) {
	c.Lock()
	defer c.Unlock()
	return c.Conn.Write(b)
}
func (f *tagRPCFaults) pump(dst, src *serializedConn) {
	defer dst.Close()
	defer src.Close()
	for {
		header := make([]byte, 7)
		if _, err := io.ReadFull(src, header); err != nil {
			return
		}
		if !bytes.Equal(header[:6], proto.MagicNumberStart) {
			return
		}
		typ := proto.MsgType(header[6])
		frame := header
		var body []byte
		if typ != proto.MsgTypeHeartbeat {
			length := make([]byte, 4)
			if _, err := io.ReadFull(src, length); err != nil {
				return
			}
			size := binary.BigEndian.Uint32(length)
			if size > 64<<20 {
				return
			}
			body = make([]byte, size)
			if _, err := io.ReadFull(src, body); err != nil {
				return
			}
			frame = append(append(header, length...), body...)
		}
		if rejection := f.reject(typ, body); rejection != nil {
			if _, err := src.Write(rejection); err != nil {
				return
			}
			continue
		}
		if _, err := dst.Write(frame); err != nil {
			return
		}
	}
}
func (f *tagRPCFaults) startProxy(t *testing.T, upstream string) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				peer, err := net.DialTimeout("tcp", upstream, time.Second)
				if err != nil {
					conn.Close()
					return
				}
				a, b := &serializedConn{Conn: conn}, &serializedConn{Conn: peer}
				go f.pump(a, b)
				f.pump(b, a)
			}()
		}
	}()
	return l.Addr().String()
}

func TestPartialTagRPCFailureDoesNotUseStaleRecipients(t *testing.T) {
	if testing.Short() {
		t.Skip("three-process delivery regression")
	}
	c := startCluster(t)
	old, next := "partial-old", "partial-new"
	receipts := &receiptLog{messages: make(map[string]map[string][]receipt)}
	for j, uid := range []string{old, next} {
		c.api(t, 0, "/user/token", map[string]any{"uid": uid, "token": "partial-test-token", "device_flag": 0, "device_level": 1}, nil)
		cl := client.New("tcp://"+c.nodes[j+1].tcp, client.WithUID(uid), client.WithToken("partial-test-token"), func(o *client.Options) error { o.AutoReconn = false; o.Timeout = 5 * time.Second; return nil })
		cl.SetOnRecv(func(p *wkproto.RecvPacket) error { receipts.add(uid, p); return nil })
		t.Cleanup(func() { cl.Close() })
		require.NoError(t, cl.Connect())
		require.NoError(t, cl.FlushTimeout(5*time.Second))
	}
	control := "partial-connection-control"
	c.api(t, 0, "/channel/subscriber_add", map[string]any{"channel_id": control, "channel_type": 2, "subscribers": []string{old, next}}, nil)
	for _, typ := range []uint8{2, 5} {
		for _, op := range []string{"add", "remove", "reset", "remove-all"} {
			t.Run(fmt.Sprintf("type%d/%s", typ, op), func(t *testing.T) {
				id := c.channelID(t, t.Name(), false)
				cmd := id + "____cmd"
				body := map[string]any{"channel_id": id, "channel_type": typ, "subscribers": []string{old}}
				c.api(t, 0, "/channel/subscriber_add", body, nil)
				for _, isCmd := range []bool{false, true} {
					no := fmt.Sprintf("%s/warm/%v", id, isCmd)
					mid := c.send(t, id, typ, isCmd, no)
					require.Eventually(t, func() bool { return receipts.count(old, no) == 1 }, 10*time.Second, 20*time.Millisecond)
					receipts.check(t, old, no, mid, 1)
				}
				for _, channel := range []string{id, cmd} {
					require.ElementsMatch(t, []string{old}, c.tagMembers(t, channel, typ))
				}
				c.faults.Lock()
				c.faults.channel = cmd
				before := c.faults.rejected
				c.faults.Unlock()
				t.Cleanup(func() { c.faults.Lock(); c.faults.channel = ""; c.faults.Unlock() })
				path := "/channel/subscriber_add"
				var want []string
				switch op {
				case "add":
					body["subscribers"] = []string{next}
					want = []string{old, next}
				case "remove":
					path = "/channel/subscriber_remove"
				case "reset":
					body["reset"] = 1
					body["subscribers"] = []string{next}
				case "remove-all":
					path = "/channel/subscriber_remove_all"
				}
				require.Error(t, c.request(0, path, body, nil), "API must report the injected partial failure")
				c.faults.Lock()
				rejected := c.faults.rejected - before
				c.faults.Unlock()
				require.Positive(t, rejected)
				var stored []string
				c.api(t, c.owner(id), fmt.Sprintf("/cluster/channels/%s/%d/subscribers", url.PathEscape(id), typ), nil, &stored)
				require.ElementsMatch(t, want, stored)
				messages := make(map[string]int64)
				for j := 0; j < 4; j++ {
					no := fmt.Sprintf("%s/after/%d", id, j)
					messages[no] = c.send(t, id, typ, j != 0, no)
				}
				marker := id + "/control"
				markerID := c.send(t, control, 2, false, marker)
				require.Eventually(t, func() bool { return receipts.count(old, marker) == 1 && receipts.count(next, marker) == 1 }, 10*time.Second, 20*time.Millisecond)
				if op == "add" {
					require.Eventually(t, func() bool {
						for no := range messages {
							if receipts.count(old, no) != 1 || receipts.count(next, no) != 1 {
								return false
							}
						}
						return true
					}, 10*time.Second, 20*time.Millisecond)
				}
				// Continuously assert throughout the bounded late-delivery window.
				deadline := time.Now().Add(2 * time.Second)
				for time.Now().Before(deadline) {
					for no, mid := range messages {
						n := 0
						if op == "add" {
							n = 1
						}
						receipts.check(t, old, no, mid, n)
						receipts.check(t, next, no, mid, n)
					}
					if t.Failed() {
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
				for _, uid := range []string{old, next} {
					receipts.check(t, uid, marker, markerID, 1)
				}
				t.Logf("injected RPC rejections=%d durable members=%v post-failure sends=4 old cmd receipts=%d", rejected, stored, receipts.cmdCount(old, id))
			})
		}
	}
}

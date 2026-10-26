//go:build integration

package handler_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	channelevent "github.com/WuKongIM/WuKongIM/internal/channel/event"
	"github.com/WuKongIM/WuKongIM/internal/channel/handler"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/manager"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// These subprocesses isolate the application's node-global services while using
// the real channel workers, snapshot codec, and TCP RPC implementation. This is
// a transport integration test, not the 80,000-session deployment benchmark.
type wireCluster struct {
	icluster.ICluster
	server     *wkserver.Server
	mu         sync.RWMutex
	clients    map[uint64]*client.Client
	requests   atomic.Int32
	completed  atomic.Int32
	tagLookups atomic.Int32
	delay      bool
	release    <-chan struct{}
	stop       <-chan struct{}
}

func (*wireCluster) GetSlotId(uid string) uint32 {
	if len(uid) > 0 && uid[0] >= '1' && uid[0] <= '3' {
		return uint32(uid[0] - '0')
	}
	return 1
}
func (*wireCluster) SlotLeaderId(slot uint32) uint64                     { return uint64(slot) }
func (*wireCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) { return 1, nil }
func (c *wireCluster) Route(path string, fn wkserver.Handler) {
	if path == "/wk/forward/distribution/v1" && c.delay {
		c.server.Route(path, func(ctx *wkserver.Context) {
			if c.requests.Add(1) == 1 {
				select {
				case <-c.release:
				case <-c.stop:
					return
				}
			}
			fn(ctx)
			c.completed.Add(1)
		})
		return
	}
	c.server.Route(path, fn)
}
func (c *wireCluster) RequestWithContext(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	if path == "/wk/ingress/getTag" {
		c.tagLookups.Add(1)
	}
	c.mu.RLock()
	peer := c.clients[node]
	c.mu.RUnlock()
	if peer == nil {
		return nil, fmt.Errorf("peer %d unavailable", node)
	}
	return peer.RequestWithContext(ctx, path, body)
}
func (c *wireCluster) Send(node uint64, msg *proto.Message) error {
	c.mu.RLock()
	peer := c.clients[node]
	c.mu.RUnlock()
	if peer == nil {
		return fmt.Errorf("peer %d unavailable", node)
	}
	return peer.Send(msg)
}

type wirePusher struct {
	eventbus.IPusher
	mu     sync.Mutex
	counts map[int64]map[string]int
}

func (*wirePusher) RandHandlerId() int { return 1 }
func (*wirePusher) Advance(int)        {}
func (p *wirePusher) AddEvent(_ int, e *eventbus.Event) {
	if e.Type != eventbus.EventPushOnline {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.counts[e.MessageId] == nil {
		p.counts[e.MessageId] = make(map[string]int)
	}
	p.counts[e.MessageId][e.ToUid]++
}

type wireReady struct{ HTTP, RPC string }
type wireStats struct {
	Counts       map[int64]map[string]int
	Requests     int32
	Completed    int32
	TagLookups   int32
	OldAvailable bool
}

func TestDistributionIntegrationNode(t *testing.T) {
	nodeText := os.Getenv("WK_TEST_DISTRIBUTION_NODE")
	if nodeText == "" {
		return
	}
	node, err := strconv.ParseUint(nodeText, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	options.G = options.New()
	options.G.Cluster.NodeId = node
	options.G.Poller.ChannelCount = 1
	options.G.Poller.ChannelGoroutine = 8
	options.G.Poller.IntervalTick = 5 * time.Millisecond
	release := make(chan struct{})
	var releaseOnce sync.Once
	rpcAddr := os.Getenv("WK_TEST_DISTRIBUTION_RPC")
	c := &wireCluster{server: wkserver.New(rpcAddr), clients: make(map[uint64]*client.Client), delay: node == 2, release: release, stop: ctx.Done()}
	service.Cluster = c
	m := manager.NewTagManager(4, func() uint64 { return 0 })
	service.TagManager = m
	if node == 1 {
		if _, err := m.MakeTagWithTagKey("A", []string{"1-fixed", "2-fixed", "2-removed", "3-fixed"}); err != nil {
			t.Fatal(err)
		}
		m.SetChannelTag("room", wkproto.ChannelTypeData, "A")
	}
	eventbus.RegisterUser(recoveryUsers{})
	pusher := &wirePusher{counts: make(map[int64]map[string]int)}
	eventbus.RegisterPusher(pusher)
	h := handler.NewHandler()
	h.SetForwardRoutes()
	pool := channelevent.NewEventPool(h)
	eventbus.RegisterChannel(pool)
	if err := c.server.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.server.Stop()
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}
	defer pool.Stop()
	defer func() {
		for _, peer := range c.clients {
			peer.Stop()
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/peers", func(w http.ResponseWriter, r *http.Request) {
		var peers map[uint64]string
		if err := json.NewDecoder(r.Body).Decode(&peers); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		for id, addr := range peers {
			if id == node {
				continue
			}
			peer := client.New(addr, client.WithUid(nodeText))
			if err := peer.Start(); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			c.mu.Lock()
			c.clients[id] = peer
			c.mu.Unlock()
			tick := time.NewTicker(10 * time.Millisecond)
			deadline := time.NewTimer(3 * time.Second)
			for !peer.IsAuthed() {
				select {
				case <-tick.C:
				case <-deadline.C:
					tick.Stop()
					http.Error(w, "peer auth timeout", 500)
					return
				}
			}
			tick.Stop()
			deadline.Stop()
		}
		w.WriteHeader(204)
	})
	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		var id int64
		if err := json.NewDecoder(r.Body).Decode(&id); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		pool.AddEvent("room", wkproto.ChannelTypeData, &eventbus.Event{
			Type: eventbus.EventChannelDistribute, MessageId: id,
			Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{},
		})
		pool.Advance("room", wkproto.ChannelTypeData)
		w.WriteHeader(204)
	})
	mux.HandleFunc("/churn", func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 4100; i++ {
			if err := m.UpdateChannelTag("room", wkproto.ChannelTypeData, []string{"2-churn"}, i%2 == 1); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
		}
		if err := m.UpdateChannelTag("room", wkproto.ChannelTypeData, []string{"2-removed"}, true); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if err := m.UpdateChannelTag("room", wkproto.ChannelTypeData, []string{"2-new"}, false); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(204)
	})
	mux.HandleFunc("/release", func(w http.ResponseWriter, _ *http.Request) {
		releaseOnce.Do(func() { close(release) })
		w.WriteHeader(204)
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		pusher.mu.Lock()
		defer pusher.mu.Unlock()
		_ = json.NewEncoder(w).Encode(wireStats{Counts: pusher.counts, Requests: c.requests.Load(), Completed: c.completed.Load(), TagLookups: c.tagLookups.Load(), OldAvailable: m.Exist("A")})
	})
	control := httptest.NewServer(mux)
	defer control.Close()
	ready, _ := json.Marshal(wireReady{HTTP: control.URL, RPC: rpcAddr})
	fmt.Println("DISTRIBUTION_NODE_READY " + string(ready))
	<-ctx.Done()
}

func startWireNode(t *testing.T, id int) wireReady {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rpc := "tcp://" + listener.Addr().String()
	_ = listener.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDistributionIntegrationNode$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), fmt.Sprintf("WK_TEST_DISTRIBUTION_NODE=%d", id), "WK_TEST_DISTRIBUTION_RPC="+rpc)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	ready := make(chan wireReady, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if line := scanner.Text(); strings.HasPrefix(line, "DISTRIBUTION_NODE_READY ") {
				var node wireReady
				if json.Unmarshal([]byte(strings.TrimPrefix(line, "DISTRIBUTION_NODE_READY ")), &node) == nil {
					ready <- node
				}
			}
		}
	}()
	select {
	case node := <-ready:
		return node
	case <-done:
		t.Fatalf("node %d exited before readiness", id)
	case <-time.After(5 * time.Second):
		t.Fatalf("node %d startup timeout", id)
	}
	return wireReady{}
}

func wireControl(t *testing.T, node wireReady, path string, input, output any) {
	t.Helper()
	method := http.MethodGet
	var body []byte
	if input != nil {
		method = http.MethodPost
		body, _ = json.Marshal(input)
	}
	req, err := http.NewRequest(method, node.HTTP+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("node %s %s: HTTP %d", node.RPC, path, resp.StatusCode)
	}
	if output != nil {
		if err := json.NewDecoder(resp.Body).Decode(output); err != nil {
			t.Fatal(err)
		}
	}
}

func waitWireStats(t *testing.T, node wireReady, predicate func(wireStats) bool) wireStats {
	t.Helper()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	var stats wireStats
	for {
		wireControl(t, node, "/stats", nil, &stats)
		if predicate(stats) {
			return stats
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("node %s did not reach expected state: %+v", node.RPC, stats)
		}
	}
}

func TestDistributionThreeNodeSlowPeerAndVersionEviction(t *testing.T) {
	nodes := []wireReady{startWireNode(t, 1), startWireNode(t, 2), startWireNode(t, 3)}
	peers := map[uint64]string{1: nodes[0].RPC, 2: nodes[1].RPC, 3: nodes[2].RPC}
	for _, node := range nodes {
		wireControl(t, node, "/peers", peers, nil)
	}
	wireControl(t, nodes[0], "/send", int64(101), nil)
	waitWireStats(t, nodes[1], func(s wireStats) bool { return s.Requests >= 1 })
	wireControl(t, nodes[0], "/churn", struct{}{}, nil)
	var source wireStats
	wireControl(t, nodes[0], "/stats", nil, &source)
	if source.OldAvailable {
		t.Fatal("fixture did not evict old version A")
	}
	wireControl(t, nodes[0], "/send", int64(102), nil)
	waitWireStats(t, nodes[1], func(s wireStats) bool { return s.Counts[101]["2-fixed"] == 1 && s.Counts[102]["2-new"] == 1 })
	wireControl(t, nodes[1], "/release", nil, nil)
	waitWireStats(t, nodes[1], func(s wireStats) bool { return s.Completed >= 3 })
	for index, node := range nodes {
		uid := fmt.Sprintf("%d-fixed", index+1)
		stats := waitWireStats(t, node, func(s wireStats) bool { return s.Counts[101][uid] == 1 && s.Counts[102][uid] == 1 })
		if stats.TagLookups != 0 {
			t.Fatalf("node %d queried an evicted key: %+v", index+1, stats)
		}
		if index == 1 {
			if stats.Requests < 3 {
				t.Fatalf("did not exercise ambiguous admission retry: %+v", stats)
			}
			if stats.Counts[101]["2-removed"] != 1 || stats.Counts[101]["2-new"] != 0 || stats.Counts[102]["2-removed"] != 0 {
				t.Fatalf("recipient version contamination: %+v", stats.Counts)
			}
		}
		for _, counts := range stats.Counts {
			for member, count := range counts {
				if count != 1 {
					t.Fatalf("duplicate to %s: %d", member, count)
				}
			}
		}
	}
}

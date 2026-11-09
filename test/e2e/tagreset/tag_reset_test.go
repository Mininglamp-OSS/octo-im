package tagreset_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/client"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// This test uses isolated server processes and authenticated TCP receivers. It
// never connects to a deployed cluster. Set OCTO_IM_TEST_BINARY to compare an
// older binary; otherwise it builds the current checkout. Optional
// OCTO_IM_TEST_ARTIFACT_DIR retains logs/data outside t.TempDir for diagnosis.
func TestResetAndRemoveAllStopCmdDeliveryToOldMembers(t *testing.T) {
	if testing.Short() {
		t.Skip("three-process delivery regression")
	}
	c := startCluster(t)
	old, next := "tag-reset-old", "tag-reset-new"
	receipts := &receiptLog{messages: make(map[string]map[string][]receipt)}
	for i, uid := range []string{old, next} {
		c.api(t, 0, "/user/token", map[string]any{"uid": uid, "token": "synthetic-tag-reset-token", "device_flag": 0, "device_level": 1}, nil)
		cl := client.New("tcp://"+c.nodes[i+1].tcp,
			client.WithUID(uid), client.WithToken("synthetic-tag-reset-token"), func(o *client.Options) error {
				o.AutoReconn = false
				o.Timeout, o.FlusherTimeout = 5*time.Second, 5*time.Second
				return nil
			})
		cl.SetOnRecv(func(p *wkproto.RecvPacket) error {
			receipts.add(uid, p)
			return nil
		})
		t.Cleanup(func() { cl.Close() })
		require.NoError(t, cl.Connect())
		require.NoError(t, cl.FlushTimeout(5*time.Second))
	}
	control := "tag-reset-connection-control"
	c.api(t, 0, "/channel/subscriber_add", map[string]any{"channel_id": control, "channel_type": 2, "subscribers": []string{old, next}}, nil)
	for _, typ := range []uint8{2, 5} {
		for _, sameOwner := range []bool{true, false} {
			for _, operation := range []string{"reset", "remove-all"} {
				t.Run(fmt.Sprintf("type%d/same-owner%v/%s", typ, sameOwner, operation), func(t *testing.T) {
					id := c.channelID(t, t.Name(), sameOwner)
					cmdID := id + "____cmd"
					t.Logf("ordinary slot owner=%d; cmd slot owner=%d", c.owner(id)+1001, c.owner(cmdID)+1001)
					membership := map[string]any{"channel_id": id, "channel_type": typ, "subscribers": []string{old}}
					c.api(t, 0, "/channel/subscriber_add", membership, nil)
					// Warm BOTH ordinary and cmd caches through real delivery.
					for _, cmd := range []bool{false, true} {
						no := fmt.Sprintf("%s/warm/%v", id, cmd)
						mid := c.send(t, id, typ, cmd, no)
						require.Eventually(t, func() bool { return receipts.count(old, no) == 1 }, 10*time.Second, 50*time.Millisecond)
						receipts.check(t, old, no, mid, 1)
						receipts.check(t, next, no, mid, 0)
					}
					for _, channelID := range []string{id, cmdID} {
						require.ElementsMatch(t, []string{old}, c.tagMembers(t, channelID, typ), "must start with a warm authoritative Tag")
					}
					var want []string
					if operation == "reset" {
						membership["reset"], membership["subscribers"] = 1, []string{next}
						c.api(t, 0, "/channel/subscriber_add", membership, nil)
						want = []string{next}
					} else {
						c.api(t, 0, "/channel/subscriber_remove_all", membership, nil)
					}
					var stored []string
					c.api(t, c.owner(id), fmt.Sprintf("/cluster/channels/%s/%d/subscribers", url.PathEscape(id), typ), nil, &stored)
					require.ElementsMatch(t, want, stored)
					// Send only AFTER the membership API has acknowledged success.
					// System HTTP sends are accepted even with no remaining members;
					// a rejected/non-member send cannot produce a false zero-delivery pass.
					messages := make(map[string]int64)
					for j := 0; j < 4; j++ {
						no := fmt.Sprintf("%s/after/%d", id, j)
						messages[no] = c.send(t, id, typ, j != 0, no)
					}
					marker := id + "/connection-control"
					markerID := c.send(t, control, 2, false, marker)
					require.Eventually(t, func() bool {
						return receipts.count(old, marker) == 1 && receipts.count(next, marker) == 1
					}, 10*time.Second, 50*time.Millisecond, "both original TCP sessions must remain usable")
					if operation == "reset" {
						require.Eventually(t, func() bool {
							for no := range messages {
								if receipts.count(next, no) != 1 {
									return false
								}
							}
							return true
						}, 10*time.Second, 50*time.Millisecond)
					}
					// Observe forbidden late delivery, including after remove-all.
					time.Sleep(2 * time.Second)
					for _, channelID := range []string{id, cmdID} {
						assert.ElementsMatch(t, want, c.tagMembers(t, channelID, typ))
					}
					for no, mid := range messages {
						receipts.check(t, old, no, mid, 0)
						expected := 0
						if operation == "reset" {
							expected = 1
						}
						receipts.check(t, next, no, mid, expected)
					}
					for _, uid := range []string{old, next} {
						receipts.check(t, uid, marker, markerID, 1)
					}
					t.Logf("post-operation sends=%d (3 cmd + 1 ordinary); old-member cmd receipts=%d", len(messages), receipts.cmdCount(old, id))
				})
			}
		}
	}
}

type receipt struct {
	id      int64
	payload string
}
type receiptLog struct {
	sync.Mutex
	messages map[string]map[string][]receipt
}

func (r *receiptLog) add(uid string, p *wkproto.RecvPacket) {
	r.Lock()
	defer r.Unlock()
	if r.messages[uid] == nil {
		r.messages[uid] = make(map[string][]receipt)
	}
	r.messages[uid][p.ClientMsgNo] = append(r.messages[uid][p.ClientMsgNo], receipt{p.MessageID, string(p.Payload)})
}
func (r *receiptLog) count(uid, no string) int {
	r.Lock()
	defer r.Unlock()
	return len(r.messages[uid][no])
}
func (r *receiptLog) cmdCount(uid, id string) int {
	n := 0
	for j := 1; j < 4; j++ {
		n += r.count(uid, fmt.Sprintf("%s/after/%d", id, j))
	}
	return n
}
func (r *receiptLog) check(t *testing.T, uid, no string, id int64, count int) {
	t.Helper()
	r.Lock()
	defer r.Unlock()
	got := r.messages[uid][no]
	assert.Len(t, got, count, "uid=%s message=%s", uid, no)
	for _, p := range got {
		assert.Equal(t, id, p.id)
		assert.Equal(t, no, p.payload)
	}
}

type testNode struct {
	root, api, tcp string
	cmd            *exec.Cmd
	done           chan error
}
type testCluster struct {
	nodes  []testNode
	owners map[uint32]int
	http   *http.Client
}

func (c *testCluster) owner(id string) int { return c.owners[crc32.ChecksumIEEE([]byte(id))%64] }
func (c *testCluster) channelID(t *testing.T, prefix string, same bool) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("reset-%x-%d", crc32.ChecksumIEEE([]byte(prefix)), i)
		if (c.owner(id) == c.owner(id+"____cmd")) == same {
			return id
		}
	}
	t.Fatal("cannot find requested ordinary/cmd owner placement")
	return ""
}
func (c *testCluster) request(node int, path string, body, result any) error {
	method := http.MethodGet
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader, method = bytes.NewReader(b), http.MethodPost
	}
	req, err := http.NewRequest(method, c.nodes[node].api+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, b)
	}
	if body != nil {
		var status struct {
			Status int `json:"status"`
		}
		if err := json.Unmarshal(b, &status); err != nil {
			return err
		}
		if status.Status != 0 && status.Status != 200 {
			return fmt.Errorf("%s: %s", path, b)
		}
	}
	if result != nil {
		return json.Unmarshal(b, result)
	}
	return nil
}
func (c *testCluster) api(t *testing.T, node int, path string, body, result any) {
	t.Helper()
	require.NoError(t, c.request(node, path, body, result))
}
func (c *testCluster) send(t *testing.T, id string, typ uint8, cmd bool, no string) int64 {
	t.Helper()
	syncOnce := 0
	if cmd {
		syncOnce = 1
	}
	var resp struct {
		Data struct {
			ID int64 `json:"message_id"`
		} `json:"data"`
	}
	c.api(t, 0, "/message/send", map[string]any{"channel_id": id, "channel_type": typ, "client_msg_no": no, "payload": []byte(no), "header": map[string]int{"sync_once": syncOnce}}, &resp)
	require.Positive(t, resp.Data.ID, "persistent send must be admitted")
	return resp.Data.ID
}
func (c *testCluster) tagMembers(t *testing.T, id string, typ uint8) []string {
	t.Helper()
	var tag struct {
		Nodes []struct {
			Uids []string `json:"uids"`
		} `json:"nodes"`
	}
	c.api(t, c.owner(id), "/tag?"+url.Values{"channel_id": {id}, "channel_type": {fmt.Sprint(typ)}}.Encode(), nil, &tag)
	var users []string
	for _, node := range tag.Nodes {
		users = append(users, node.Uids...)
	}
	return users
}

func startCluster(t *testing.T) *testCluster {
	t.Helper()
	root := t.TempDir()
	if parent := os.Getenv("OCTO_IM_TEST_ARTIFACT_DIR"); parent != "" {
		require.NoError(t, os.MkdirAll(parent, 0700))
		var err error
		root, err = os.MkdirTemp(parent, "tag-reset-")
		require.NoError(t, err)
	}
	t.Logf("cluster artifacts: %s", root)
	binary := os.Getenv("OCTO_IM_TEST_BINARY")
	if binary == "" {
		binary = filepath.Join(root, "octo-im")
		build := exec.Command("go", "build", "-o", binary, "main.go")
		build.Dir = "../../.."
		out, err := build.CombinedOutput()
		require.NoError(t, err, "build server: %s", out)
	}
	c := &testCluster{owners: make(map[uint32]int), http: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}}
	// Reserve every port together to avoid accidental duplicates and preserve
	// per-process isolation when go test runs other packages concurrently.
	var listeners []net.Listener
	var ports []string
	for i := 0; i < 15; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().String())
	}
	t.Cleanup(func() {
		for _, l := range listeners {
			_ = l.Close()
		}
		c.http.CloseIdleConnections()
	})
	initNodes := make([]string, 3)
	for i := 0; i < 3; i++ {
		initNodes[i] = fmt.Sprintf("%d@%s", 1001+i, ports[i*5+3])
	}
	for i := 0; i < 3; i++ {
		dir := filepath.Join(root, fmt.Sprintf("node%d", 1001+i))
		require.NoError(t, os.MkdirAll(dir, 0700))
		config := map[string]any{
			"mode": "release", "rootDir": dir, "addr": "tcp://" + ports[i*5+1], "httpAddr": ports[i*5], "wsAddr": "ws://" + ports[i*5+2],
			"external": map[string]any{"ip": "127.0.0.1", "tcpAddr": ports[i*5+1]}, "tokenAuthOn": true, "pprofOn": false,
			"logger":  map[string]any{"level": 2, "traceOn": false, "dir": filepath.Join(dir, "logs")},
			"manager": map[string]any{"on": false, "addr": ports[i*5+4]}, "demo": map[string]any{"on": false}, "auth": map[string]any{"kind": "none"},
			"conversation": map[string]any{"on": false}, "db": map[string]any{"shardNum": 2, "slotShardNum": 2},
			"cluster": map[string]any{"nodeId": 1001 + i, "addr": "tcp://" + ports[i*5+3], "serverAddr": ports[i*5+3], "apiUrl": "http://" + ports[i*5], "slotCount": 64, "slotReplicaCount": 3, "channelReplicaCount": 3, "initNodes": initNodes},
			"plugin":  map[string]any{"socketPath": fmt.Sprintf("/tmp/tagreset-%d-%s.sock", os.Getpid(), ports[i*5][10:])},
		}
		data, err := yaml.Marshal(config)
		require.NoError(t, err)
		cfg := filepath.Join(dir, "config.yaml")
		require.NoError(t, os.WriteFile(cfg, data, 0600))
		log, err := os.Create(filepath.Join(dir, "server.log"))
		require.NoError(t, err)
		cmd := exec.Command(binary, "--config", cfg)
		cmd.Stdout, cmd.Stderr = log, log
		cmd.Env = append(os.Environ(), "GOMAXPROCS=2")
		for _, l := range listeners[i*5 : i*5+5] {
			require.NoError(t, l.Close())
		}
		require.NoError(t, cmd.Start())
		node := testNode{root: dir, api: "http://" + ports[i*5], tcp: ports[i*5+1], cmd: cmd, done: make(chan error, 1)}
		go func() { node.done <- cmd.Wait() }()
		c.nodes = append(c.nodes, node)
		t.Cleanup(func() {
			_ = cmd.Process.Signal(os.Interrupt)
			select {
			case <-node.done:
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
				<-node.done
			}
			_ = log.Close()
		})
	}
	type slot struct {
		ID                     uint32 `json:"id"`
		Leader                 uint64 `json:"leader"`
		Replicas, Learners     []uint64
		MigrateFrom, MigrateTo uint64
		Status                 int
	}
	require.Eventually(t, func() bool {
		var first []slot
		for i, node := range c.nodes {
			b, err := os.ReadFile(filepath.Join(node.root, "data/cluster/config/remote.json"))
			if err != nil {
				return false
			}
			var cfg struct {
				Slots []slot `json:"slots"`
				Nodes []struct {
					ID     uint64 `json:"id"`
					API    string `json:"apiServerAddr"`
					Online bool   `json:"online"`
				} `json:"nodes"`
			}
			if json.Unmarshal(b, &cfg) != nil || len(cfg.Slots) != 64 {
				return false
			}
			if len(cfg.Nodes) != 3 {
				return false
			}
			for _, n := range cfg.Nodes {
				if n.ID < 1001 || n.ID > 1003 || !n.Online || n.API != c.nodes[n.ID-1001].api {
					return false
				}
			}
			if i == 0 {
				first = cfg.Slots
			} else if !reflect.DeepEqual(first, cfg.Slots) {
				return false
			}
			if c.request(i, "/health", nil, nil) != nil {
				return false
			}
		}
		for _, s := range first {
			if s.Leader < 1001 || s.Leader > 1003 || len(s.Replicas) != 3 || len(s.Learners) != 0 || s.MigrateFrom != 0 || s.MigrateTo != 0 || s.Status != 0 {
				return false
			}
			c.owners[s.ID] = int(s.Leader) - 1001
		}
		return true
	}, 90*time.Second, 250*time.Millisecond, "three nodes and all 64 slots must converge")
	return c
}

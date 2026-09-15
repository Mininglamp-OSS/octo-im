package presence

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

type testSocket struct {
	wknet.Conn
	id  int64
	ctx interface{}
}

func (c *testSocket) ID() int64                { return c.id }
func (c *testSocket) SetContext(v interface{}) { c.ctx = v }
func (c *testSocket) Context() interface{}     { return c.ctx }

type testUsers struct {
	eventbus.IUser
	mu    sync.Mutex
	conns []*eventbus.Conn
}

func (u *testUsers) ConnsByUid(uid string) []*eventbus.Conn {
	u.mu.Lock()
	defer u.mu.Unlock()
	var out []*eventbus.Conn
	for _, c := range u.conns {
		if c.Uid == uid {
			out = append(out, c)
		}
	}
	return out
}
func (u *testUsers) UpdateConn(conn *eventbus.Conn) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for i, c := range u.conns {
		if c.Equal(conn) {
			u.conns[i] = conn
			return
		}
	}
	u.conns = append(u.conns, conn)
}
func (u *testUsers) RemoveConn(conn *eventbus.Conn) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for i, c := range u.conns {
		if c.SameSession(conn) {
			u.conns = append(u.conns[:i], u.conns[i+1:]...)
			return
		}
	}
}

type testCluster struct {
	icluster.ICluster
	owner     *Manager
	leader    uint64
	version   uint64
	request   func() error
	afterRead func()
}

func (c *testCluster) GetSlotId(string) uint32    { return 19 }
func (c *testCluster) SlotLeaderId(uint32) uint64 { return c.leader }
func (c *testCluster) NodeVersion() uint64        { return c.version }
func (c *testCluster) Nodes() []*types.Node {
	return []*types.Node{{Id: 1, Online: true}, {Id: 2, Online: true}}
}
func (c *testCluster) RequestWithContext(ctx context.Context, _ uint64, path string, body []byte) (*proto.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.request != nil {
		if err := c.request(); err != nil {
			return nil, err
		}
	}
	var uids []string
	if err := json.Unmarshal(body, &uids); err != nil {
		return nil, err
	}
	response, err := c.owner.snapshot(uids)
	if err != nil {
		return nil, err
	}
	if c.afterRead != nil {
		c.afterRead()
	}
	data, _ := json.Marshal(response)
	return &proto.Response{Status: proto.StatusOK, Body: data}, nil
}

func fixture(t *testing.T) (*Manager, *Manager, *testCluster, *testUsers, *testSocket, *eventbus.Conn) {
	oldCluster, oldUser := service.Cluster, eventbus.User
	t.Cleanup(func() { service.Cluster, eventbus.User = oldCluster, oldUser })
	owner, leader := New(1), New(2)
	cluster := &testCluster{owner: owner, leader: 2, version: 1}
	service.Cluster = cluster
	users := &testUsers{}
	eventbus.RegisterUser(users)
	raw := &testSocket{id: 7}
	conn := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web"}
	owner.Track(raw)
	owner.Prepare(raw, conn)
	conn.Auth = true
	require.True(t, owner.Authenticate(conn))
	return owner, leader, cluster, users, raw, conn
}

func TestNewAuthorityRecoversUnchangedPhysicalSocket(t *testing.T) {
	_, leader, _, users, raw, conn := fixture(t)
	require.Empty(t, users.ConnsByUid("u"))
	require.NoError(t, leader.Recover(context.Background(), []string{"u"}))
	got := users.ConnsByUid("u")
	require.Len(t, got, 1)
	require.True(t, got[0].SameSession(conn))
	require.Same(t, conn, raw.Context())
	require.True(t, got[0].Auth)
}

func TestCloseDuringRecoveryCannotResurrectSession(t *testing.T) {
	owner, leader, cluster, users, raw, conn := fixture(t)
	once := sync.Once{}
	cluster.afterRead = func() { once.Do(func() { owner.Close(raw); leader.Forget(conn) }) }
	require.NoError(t, leader.Recover(context.Background(), []string{"u"}))
	require.Empty(t, users.ConnsByUid("u"))
	_, err := leader.Verify(context.Background(), conn)
	require.Error(t, err)
}

func TestRecoveryFailsClosedWhenAnOwnerCannotBeRead(t *testing.T) {
	_, leader, cluster, users, _, _ := fixture(t)
	cluster.request = func() error { return errors.New("owner unreachable") }
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	require.Error(t, leader.Recover(ctx, []string{"u"}))
	require.Empty(t, users.ConnsByUid("u"))
	cluster.request = nil
	require.NoError(t, leader.Recover(context.Background(), []string{"u"}))
	require.Len(t, users.ConnsByUid("u"), 1)
}

func TestSessionIdentityFencesIDReuseAndOwnerRestart(t *testing.T) {
	owner, leader, _, _, oldRaw, oldConn := fixture(t)
	owner.Close(oldRaw)
	newRaw := &testSocket{id: 7}
	newConn := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7}
	owner.Track(newRaw)
	owner.Prepare(newRaw, newConn)
	newConn.Auth = true
	require.False(t, owner.Authenticate(oldConn))
	require.True(t, owner.Authenticate(newConn))
	owner.Close(oldRaw)
	_, err := leader.Verify(context.Background(), oldConn)
	require.Error(t, err)
	verified, err := leader.Verify(context.Background(), newConn)
	require.NoError(t, err)
	require.True(t, verified.SameSession(newConn))
	restarted := New(1)
	restarted.Track(newRaw)
	restarted.Prepare(newRaw, &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7})
	require.False(t, restarted.Authenticate(newConn))
}

func TestForgedDescriptorAndStaleAuthorityAreRejected(t *testing.T) {
	_, leader, cluster, users, _, conn := fixture(t)
	forged := copyConn(conn)
	forged.SessionID = "forged"
	_, err := leader.Verify(context.Background(), forged)
	require.Error(t, err)
	cluster.leader = 1
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	require.Error(t, leader.Recover(ctx, []string{"u"}))
	require.Empty(t, users.ConnsByUid("u"))
}

func TestRecoveryRepairsAnEvictedLogicalView(t *testing.T) {
	_, leader, _, users, _, conn := fixture(t)
	require.NoError(t, leader.Recover(context.Background(), []string{"u"}))
	users.RemoveConn(conn)
	require.Empty(t, users.ConnsByUid("u"))
	require.NoError(t, leader.Recover(context.Background(), []string{"u"}))
	require.Len(t, users.ConnsByUid("u"), 1)
}

func TestRegistryReindexesReplacedSocketsAndPendingIdentities(t *testing.T) {
	owner, _, _, _, oldRaw, oldConn := fixture(t)
	replacement := &testSocket{id: oldRaw.ID()}
	owner.Track(replacement)
	require.Empty(t, owner.byUID)
	pending := &eventbus.Conn{Uid: "pending", NodeId: 1, ConnId: replacement.ID()}
	owner.Prepare(replacement, pending)
	pending.Uid = "next"
	owner.Prepare(replacement, pending)
	require.Len(t, owner.byUID, 1)
	require.Contains(t, owner.byUID, "next")
	require.False(t, owner.Authenticate(oldConn))
	owner.Close(oldRaw)
	require.Len(t, owner.physical, 1)
	owner.Close(replacement)
	require.Empty(t, owner.byUID)
}

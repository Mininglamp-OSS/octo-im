package presence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

type testSocket struct {
	wknet.Conn
	id     int64
	ctx    interface{}
	uptime time.Time
}

type testSocketWrapper struct {
	wknet.Conn
}

func (c *testSocket) ID() int64                { return c.id }
func (c *testSocket) SetContext(v interface{}) { c.ctx = v }
func (c *testSocket) Context() interface{}     { return c.ctx }
func (c *testSocket) Uptime() time.Time {
	if c.uptime.IsZero() {
		return time.Unix(0, c.id)
	}
	return c.uptime
}

type testUsers struct {
	eventbus.IUser
	mu     sync.Mutex
	conns  []*eventbus.Conn
	events []*eventbus.Event
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
func (u *testUsers) UpdateConnRecovered(conn *eventbus.Conn) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for i, current := range u.conns {
		if !current.Equal(conn) {
			continue
		}
		if !current.SameSession(conn) {
			return
		}
		u.conns[i] = conn
		return
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
func (u *testUsers) AddEvent(_ string, event *eventbus.Event) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.events = append(u.events, event)
}
func (u *testUsers) Advance(string) {}

type testCluster struct {
	icluster.ICluster
	owner       *Manager
	leader      uint64
	version     uint64
	request     func() error
	requestCtx  func(context.Context) error
	requestNode func(uint64) error
	afterRead   func()
	mutate      func(*snapshotResponse)
	nodes       []*types.Node
}

func (c *testCluster) GetSlotId(string) uint32    { return 19 }
func (c *testCluster) SlotLeaderId(uint32) uint64 { return c.leader }
func (c *testCluster) NodeVersion() uint64        { return c.version }
func (c *testCluster) Nodes() []*types.Node {
	if c.nodes != nil {
		return c.nodes
	}
	return []*types.Node{{Id: 1, Online: true}, {Id: 2, Online: true}}
}
func (c *testCluster) RequestWithContext(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.request != nil {
		if err := c.request(); err != nil {
			return nil, err
		}
	}
	if c.requestCtx != nil {
		if err := c.requestCtx(ctx); err != nil {
			return nil, err
		}
	}
	if c.requestNode != nil {
		if err := c.requestNode(node); err != nil {
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
	if c.mutate != nil {
		c.mutate(&response)
	}
	data, _ := json.Marshal(response)
	return &proto.Response{Status: proto.StatusOK, Body: data}, nil
}

func fixture(t *testing.T) (*Manager, *Manager, *testCluster, *testUsers, *testSocket, *eventbus.Conn) {
	oldOptions, oldCluster, oldUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = oldOptions, oldCluster, oldUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 2
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

func TestAuthenticationDuringRecoveryCannotBeEvictedByOlderSnapshot(t *testing.T) {
	owner, leader, cluster, users, _, oldConn := fixture(t)
	users.UpdateConn(oldConn)
	newRaw := &testSocket{id: 8}
	newConn := &eventbus.Conn{Uid: oldConn.Uid, NodeId: 1, ConnId: 8, DeviceId: "mobile"}

	once := sync.Once{}
	cluster.afterRead = func() {
		once.Do(func() {
			owner.Track(newRaw)
			owner.Prepare(newRaw, newConn)
			newConn.Auth = true
			require.True(t, owner.Authenticate(newConn))
			leader.Invalidate(newConn.Uid)
			users.UpdateConn(newConn)
		})
	}

	err := leader.recoverBatch(context.Background(), []string{oldConn.Uid})

	require.ErrorIs(t, err, ErrNotReady)
	got := users.ConnsByUid(oldConn.Uid)
	require.Len(t, got, 2)
	require.True(t, got[0].SameSession(oldConn) || got[1].SameSession(oldConn))
	require.True(t, got[0].SameSession(newConn) || got[1].SameSession(newConn))
}

func TestOlderConcurrentRecoveryCannotResurrectEvictedSession(t *testing.T) {
	owner, leader, cluster, users, raw, conn := fixture(t)
	require.NoError(t, leader.Recover(context.Background(), []string{conn.Uid}))
	require.Len(t, users.ConnsByUid(conn.Uid), 1)
	leader.mu.Lock()
	leader.ready[conn.Uid].until = time.Time{}
	leader.mu.Unlock()

	firstRead := make(chan struct{})
	releaseFirst := make(chan struct{})
	var hookMu sync.Mutex
	requestCount := 0
	cluster.afterRead = func() {
		hookMu.Lock()
		requestCount++
		current := requestCount
		hookMu.Unlock()
		if current == 1 {
			close(firstRead)
			<-releaseFirst
		}
	}

	olderResult := make(chan error, 1)
	go func() {
		olderResult <- leader.recoverBatch(context.Background(), []string{conn.Uid})
	}()
	<-firstRead

	owner.Close(raw)
	newerErr := leader.recoverBatch(context.Background(), []string{conn.Uid})
	close(releaseFirst)
	olderErr := <-olderResult

	require.NoError(t, newerErr)
	require.ErrorIs(t, olderErr, ErrNotReady)
	require.Empty(t, users.ConnsByUid(conn.Uid))
}

func TestPreparedSessionCannotBeEvictedBeforeOwnerAuthentication(t *testing.T) {
	owner, leader, _, users, _, oldConn := fixture(t)
	require.NoError(t, leader.Recover(context.Background(), []string{oldConn.Uid}))

	newRaw := &testSocket{id: 8}
	newConn := &eventbus.Conn{Uid: oldConn.Uid, NodeId: 1, ConnId: 8, DeviceId: "mobile"}
	owner.Track(newRaw)
	owner.Prepare(newRaw, newConn)
	snapshot, err := owner.snapshot([]string{newConn.Uid})
	require.NoError(t, err)
	require.Len(t, snapshot.PreparedSessions, 1)
	prepared := &eventbus.Conn{}
	require.NoError(t, prepared.Decode(snapshot.PreparedSessions[0]))
	require.False(t, prepared.Auth)
	require.Empty(t, prepared.AesIV)
	require.Empty(t, prepared.AesKey)
	require.False(t, prepared.SameSession(newConn))
	require.True(t, preparedSessionMatches(newConn, prepared))

	newConn.Auth = true
	leader.Invalidate(newConn.Uid)
	users.UpdateConn(newConn)
	require.NoError(t, leader.recoverBatch(context.Background(), []string{newConn.Uid}))

	got := users.ConnsByUid(newConn.Uid)
	require.Len(t, got, 2)
	require.True(t, got[0].SameSession(newConn) || got[1].SameSession(newConn))
}

func TestOneInvalidatedUIDDoesNotPoisonRecoveryBatch(t *testing.T) {
	owner, leader, cluster, users, _, first := fixture(t)
	secondRaw := &testSocket{id: 8}
	second := &eventbus.Conn{Uid: "v", NodeId: 1, ConnId: 8, DeviceId: "web"}
	owner.Track(secondRaw)
	owner.Prepare(secondRaw, second)
	second.Auth = true
	require.True(t, owner.Authenticate(second))

	once := sync.Once{}
	cluster.afterRead = func() { once.Do(func() { leader.Forget(first) }) }
	err := leader.recoverBatch(context.Background(), []string{first.Uid, second.Uid})

	require.ErrorIs(t, err, ErrNotReady)
	require.Empty(t, users.ConnsByUid(first.Uid))
	got := users.ConnsByUid(second.Uid)
	require.Len(t, got, 1)
	require.True(t, got[0].SameSession(second))
	require.True(t, leader.IsReady(second.Uid))
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

func TestPartialRecoveryPublishesKnownLiveSessionsWithoutClaimingReady(t *testing.T) {
	_, leader, cluster, users, _, conn := fixture(t)
	cluster.nodes = []*types.Node{{Id: 1, Online: true}, {Id: 2, Online: true}, {Id: 3, Online: true}}
	cluster.requestNode = func(node uint64) error {
		if node == 3 {
			return errors.New("third owner unavailable")
		}
		return nil
	}
	err := leader.Recover(context.Background(), []string{conn.Uid})
	require.Error(t, err)
	got := users.ConnsByUid(conn.Uid)
	require.Len(t, got, 1)
	require.True(t, got[0].SameSession(conn))
	require.False(t, leader.IsReady(conn.Uid))
}

func TestWarmRecoveryBypassesSaturatedColdGate(t *testing.T) {
	_, leader, _, _, _, conn := fixture(t)
	require.NoError(t, leader.Recover(context.Background(), []string{conn.Uid}))
	for i := 0; i < cap(leader.gate); i++ {
		leader.gate <- struct{}{}
	}
	t.Cleanup(func() {
		for len(leader.gate) > 0 {
			<-leader.gate
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.NoError(t, leader.Recover(ctx, []string{conn.Uid}))
}

func TestCompleteNegativeSnapshotIsCachedBriefly(t *testing.T) {
	owner, leader, _, users, raw, conn := fixture(t)
	owner.Close(raw)
	require.NoError(t, leader.Recover(context.Background(), []string{conn.Uid}))
	require.Empty(t, users.ConnsByUid(conn.Uid))
	require.True(t, leader.IsReady(conn.Uid))
}

func TestRecoveryEvictionSchedulesOfflineNotification(t *testing.T) {
	owner, leader, _, users, raw, conn := fixture(t)
	require.NoError(t, leader.Recover(context.Background(), []string{conn.Uid}))
	owner.Close(raw)
	leader.mu.Lock()
	leader.ready[conn.Uid].until = time.Time{}
	leader.mu.Unlock()
	require.NoError(t, leader.Recover(context.Background(), []string{conn.Uid}))
	require.Empty(t, users.ConnsByUid(conn.Uid))
	require.Len(t, users.events, 1)
	require.True(t, users.events[0].PresenceReconciled)
}

func TestSessionIdentityFencesIDReuseAndOwnerRestart(t *testing.T) {
	owner, leader, _, _, oldRaw, oldConn := fixture(t)
	owner.Close(oldRaw)
	newRaw := &testSocket{id: 7, uptime: oldRaw.Uptime().Add(time.Nanosecond)}
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
	replacement := &testSocket{id: oldRaw.ID(), uptime: oldRaw.Uptime().Add(time.Nanosecond)}
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

func TestPrepareKeepsOneSessionIdentityPerSocket(t *testing.T) {
	owner, _, _, _, raw, first := fixture(t)
	second := &eventbus.Conn{Uid: first.Uid, NodeId: first.NodeId, ConnId: first.ConnId, DeviceId: first.DeviceId, DeviceFlag: first.DeviceFlag, Uptime: first.Uptime}
	owner.Prepare(raw, second)
	require.Equal(t, first.OwnerBootID, second.OwnerBootID)
	require.Equal(t, first.SessionID, second.SessionID)
}

func TestPrepareBeforeTrackKeepsPreparedSession(t *testing.T) {
	owner := New(1)
	underlying := &testSocket{id: 7, uptime: time.Unix(10, 20)}
	wrapper := &testSocketWrapper{Conn: underlying}
	conn := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: underlying.ID(), DeviceId: "web"}

	owner.Prepare(underlying, conn)
	require.NotEmpty(t, conn.OwnerBootID)
	require.NotEmpty(t, conn.SessionID)
	owner.Track(wrapper)

	conn.Auth = true
	require.True(t, owner.Authenticate(conn))
	snapshot, err := owner.snapshot([]string{conn.Uid})
	require.NoError(t, err)
	require.Len(t, snapshot.Sessions, 1)
	require.Contains(t, owner.liveUIDs(), conn.Uid)
}

func TestWrappedSocketCloseRemovesPhysicalSession(t *testing.T) {
	owner := New(1)
	underlying := &testSocket{id: 7, uptime: time.Unix(10, 20)}
	wrapper := &testSocketWrapper{Conn: underlying}
	conn := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: underlying.ID(), DeviceId: "web"}

	owner.Track(wrapper)
	owner.Prepare(wrapper, conn)
	conn.Auth = true
	require.True(t, owner.Authenticate(conn))
	require.Contains(t, owner.liveUIDs(), conn.Uid)

	owner.Close(underlying)

	snapshot, err := owner.snapshot([]string{conn.Uid})
	require.NoError(t, err)
	require.Empty(t, snapshot.Sessions)
	require.NotContains(t, owner.liveUIDs(), conn.Uid)
	require.Empty(t, owner.physical)
	require.Empty(t, owner.byUID)
}

func TestStaleCloseCannotRemoveReusedConnectionID(t *testing.T) {
	owner := New(1)
	oldRaw := &testSocket{id: 7, uptime: time.Unix(10, 20)}
	owner.Track(&testSocketWrapper{Conn: oldRaw})

	newRaw := &testSocket{id: 7, uptime: time.Unix(10, 21)}
	wrapper := &testSocketWrapper{Conn: newRaw}
	conn := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: newRaw.ID(), DeviceId: "web"}
	owner.Track(wrapper)
	owner.Prepare(newRaw, conn)
	conn.Auth = true
	require.True(t, owner.Authenticate(conn))

	owner.Close(oldRaw)

	snapshot, err := owner.snapshot([]string{conn.Uid})
	require.NoError(t, err)
	require.Len(t, snapshot.Sessions, 1)
	require.Contains(t, owner.liveUIDs(), conn.Uid)
	require.Len(t, owner.physical, 1)
	require.Len(t, owner.byUID, 1)
}

func TestLegacyDescriptorCannotAuthenticateReusedConnectionID(t *testing.T) {
	owner := New(1)
	started := time.Unix(10, 20)
	oldRaw := &testSocket{id: 7, uptime: started}
	oldConn := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: 1}
	owner.Track(oldRaw)
	owner.Prepare(oldRaw, oldConn)
	staleLegacy := &eventbus.Conn{Uid: oldConn.Uid, NodeId: oldConn.NodeId, ConnId: oldConn.ConnId, DeviceId: oldConn.DeviceId, DeviceFlag: oldConn.DeviceFlag, Uptime: oldConn.Uptime, Auth: true}
	owner.Close(oldRaw)

	newRaw := &testSocket{id: 7, uptime: started}
	newConn := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: 1}
	owner.Track(newRaw)
	owner.Prepare(newRaw, newConn)
	require.NotEqual(t, oldConn.Uptime, newConn.Uptime)
	require.False(t, owner.Authenticate(staleLegacy))

	currentLegacy := &eventbus.Conn{Uid: newConn.Uid, NodeId: newConn.NodeId, ConnId: newConn.ConnId, DeviceId: newConn.DeviceId, DeviceFlag: newConn.DeviceFlag, Uptime: newConn.Uptime, Auth: true}
	require.True(t, owner.Authenticate(currentLegacy))
}

func TestSnapshotOmitsCryptoAndVerifyKeepsCallerDescriptor(t *testing.T) {
	owner, leader, _, _, _, conn := fixture(t)
	conn.AesIV = []byte("0123456789abcdef")
	conn.AesKey = []byte("abcdef0123456789")
	require.True(t, owner.Authenticate(conn))

	snapshot, err := owner.snapshot([]string{conn.Uid})
	require.NoError(t, err)
	require.Len(t, snapshot.Sessions, 1)
	redacted := &eventbus.Conn{}
	require.NoError(t, redacted.Decode(snapshot.Sessions[0]))
	require.Empty(t, redacted.AesIV)
	require.Empty(t, redacted.AesKey)

	verified, err := leader.Verify(context.Background(), conn)
	require.NoError(t, err)
	require.Same(t, conn, verified)
	require.Equal(t, []byte("0123456789abcdef"), verified.AesIV)
	require.Equal(t, []byte("abcdef0123456789"), verified.AesKey)
}

func TestVerifyRejectsForeignSnapshotBoot(t *testing.T) {
	_, leader, cluster, _, _, conn := fixture(t)
	cluster.mutate = func(response *snapshotResponse) { response.Boot = "foreign-boot" }
	_, err := leader.Verify(context.Background(), conn)
	require.ErrorIs(t, err, ErrNotReady)
}

func TestRecoveryProcessesUIDBatchesConcurrently(t *testing.T) {
	_, leader, cluster, _, _, _ := fixture(t)
	cluster.requestCtx = func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return nil
		}
	}
	uids := make([]string, 257)
	for i := range uids {
		uids[i] = fmt.Sprintf("u-%d", i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 220*time.Millisecond)
	defer cancel()
	require.NoError(t, leader.Recover(ctx, uids))
}

func TestOfflineMarkedOwnerCannotCauseLiveSessionEviction(t *testing.T) {
	_, leader, cluster, users, _, conn := fixture(t)
	require.NoError(t, leader.Recover(context.Background(), []string{conn.Uid}))
	require.Len(t, users.ConnsByUid(conn.Uid), 1)
	cluster.nodes = []*types.Node{{Id: 1, Online: false}, {Id: 2, Online: true}}
	leader.mu.Lock()
	leader.ready[conn.Uid].until = time.Time{}
	leader.mu.Unlock()

	err := leader.Recover(context.Background(), []string{conn.Uid})

	require.ErrorIs(t, err, ErrNotReady)
	got := users.ConnsByUid(conn.Uid)
	require.Len(t, got, 1)
	require.True(t, got[0].SameSession(conn))
	require.Empty(t, users.events, "an incomplete membership view must not emit a false offline event")
}

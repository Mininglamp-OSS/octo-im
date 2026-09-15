package presence

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"golang.org/x/sync/errgroup"
)

const snapshotPath = "/wk/presence/snapshot/v1"
const touchPath = "/wk/presence/touch/v1"

type snapshotResponse struct {
	Boot     string
	Sessions [][]byte
}

func (m *Manager) SetRoutes() {
	service.Cluster.Route(snapshotPath, func(c *wkserver.Context) {
		var uids []string
		if json.Unmarshal(c.Body(), &uids) != nil {
			c.WriteErr(ErrNotReady)
			return
		}
		response, err := m.snapshot(uids)
		if err != nil {
			c.WriteErr(err)
			return
		}
		data, err := json.Marshal(response)
		if err != nil {
			c.WriteErr(err)
			return
		}
		c.Write(data)
	})
	service.Cluster.Route(touchPath, func(c *wkserver.Context) {
		var uids []string
		if json.Unmarshal(c.Body(), &uids) != nil || len(uids) > 128 {
			c.WriteErr(ErrNotReady)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		// A touch carries no authentication assertion. Pull current physical
		// snapshots, so replaying an old touch cannot resurrect a closed socket.
		if err := m.Recover(ctx, uids); err != nil {
			c.WriteErr(err)
			return
		}
		c.WriteOk()
	})
}

func (m *Manager) read(ctx context.Context, node uint64, uids []string) ([]*eventbus.Conn, error) {
	var response snapshotResponse
	if node == m.node {
		var err error
		response, err = m.snapshot(uids)
		if err != nil {
			return nil, err
		}
	} else {
		body, _ := json.Marshal(uids)
		resp, err := service.Cluster.RequestWithContext(ctx, node, snapshotPath, body)
		if err != nil {
			return nil, err
		}
		if resp == nil || resp.Status != proto.StatusOK {
			return nil, ErrNotReady
		}
		if err = json.Unmarshal(resp.Body, &response); err != nil {
			return nil, err
		}
	}
	if response.Boot == "" {
		return nil, ErrNotReady
	}
	requested := map[string]bool{}
	for _, uid := range uids {
		requested[uid] = true
	}
	var conns []*eventbus.Conn
	seen := map[string]bool{}
	for _, data := range response.Sessions {
		conn := &eventbus.Conn{}
		if err := conn.Decode(data); err != nil {
			return nil, err
		}
		if !conn.Auth || !requested[conn.Uid] || conn.NodeId != node || conn.OwnerBootID != response.Boot || conn.SessionID == "" {
			return nil, ErrNotReady
		}
		if seen[conn.SessionID] {
			return nil, ErrNotReady
		}
		seen[conn.SessionID] = true
		conn.LastActive = uint64(time.Now().Unix())
		conns = append(conns, conn)
	}
	return conns, nil
}

func (m *Manager) Verify(ctx context.Context, expected *eventbus.Conn) (*eventbus.Conn, error) {
	if expected == nil || expected.SessionID == "" || expected.OwnerBootID == "" {
		return nil, ErrNotReady
	}
	conns, err := m.read(ctx, expected.NodeId, []string{expected.Uid})
	if err != nil {
		return nil, err
	}
	for _, conn := range conns {
		if conn.SameSession(expected) {
			return conn, nil
		}
	}
	return nil, ErrNotReady
}

func (m *Manager) Recover(parent context.Context, uids []string) error {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	if err := m.lockRecovery(ctx); err != nil {
		return err
	}
	defer func() { <-m.gate }()
	for offset := 0; offset < len(uids); offset += 128 {
		batch := uids[offset:min(offset+128, len(uids))]
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			err = m.recoverBatch(ctx, batch)
			if err == nil {
				break
			}
			timer := time.NewTimer(time.Duration(attempt+1) * 50 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) recoverBatch(ctx context.Context, uids []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	version := service.Cluster.NodeVersion()
	states := map[string]*readiness{}
	generations := map[string]uint64{}
	var missing []string
	m.mu.Lock()
	if len(m.ready)+len(uids) > 32768 {
		m.ready = make(map[string]*readiness)
	}
	for _, uid := range uids {
		if uid == "" {
			continue
		}
		if service.Cluster.SlotLeaderId(service.Cluster.GetSlotId(uid)) != m.node {
			m.mu.Unlock()
			return ErrNotReady
		}
		r := m.ready[uid]
		if r == nil {
			r = &readiness{}
			m.ready[uid] = r
		}
		if r.version == version && time.Now().Before(r.until) && len(eventbus.User.ConnsByUid(uid)) > 0 {
			continue
		}
		if states[uid] != nil {
			continue
		}
		states[uid] = r
		generations[uid] = r.generation
		missing = append(missing, uid)
	}
	m.mu.Unlock()
	if len(missing) == 0 {
		return nil
	}
	nodes := service.Cluster.Nodes()
	var owners []uint64
	foundLocal := false
	for _, node := range nodes {
		if node.Id == m.node {
			foundLocal = true
		}
		if node.Online || node.Id == m.node {
			owners = append(owners, node.Id)
		}
	}
	if !foundLocal {
		return ErrNotReady
	}
	results := make([][]*eventbus.Conn, len(owners))
	group, readCtx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for i, node := range owners {
		i, node := i, node
		group.Go(func() error {
			requestCtx, cancel := context.WithTimeout(readCtx, time.Second)
			defer cancel()
			conns, err := m.read(requestCtx, node, missing)
			if err != nil {
				return err
			}
			results[i] = conns
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("%w: %v", ErrNotReady, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if service.Cluster.NodeVersion() != version {
		return ErrNotReady
	}
	for uid, r := range states {
		if m.ready[uid] != r || r.generation != generations[uid] || service.Cluster.SlotLeaderId(service.Cluster.GetSlotId(uid)) != m.node {
			return ErrNotReady
		}
	}
	byUID := make(map[string][]*eventbus.Conn)
	for _, conns := range results {
		for _, conn := range conns {
			byUID[conn.Uid] = append(byUID[conn.Uid], conn)
		}
	}
	for uid, r := range states {
		current := byUID[uid]
		for _, old := range eventbus.User.ConnsByUid(uid) {
			if !old.Auth {
				continue
			}
			found := false
			for _, conn := range current {
				if old.SameSession(conn) {
					found = true
					break
				}
			}
			if !found {
				eventbus.User.DirectRemoveConn(old)
			}
		}
		for _, conn := range current {
			eventbus.User.UpdateConn(conn)
		}
		r.version = version
		r.until = time.Time{}
		if len(current) > 0 {
			r.until = time.Now().Add(5 * time.Second)
		}
	}
	return nil
}

func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			groups := map[uint64][]string{}
			for _, uid := range m.liveUIDs() {
				leader := service.Cluster.SlotLeaderId(service.Cluster.GetSlotId(uid))
				if leader != 0 {
					groups[leader] = append(groups[leader], uid)
				}
			}
			g, gctx := errgroup.WithContext(ctx)
			g.SetLimit(4)
			for node, uids := range groups {
				node, uids := node, uids
				g.Go(func() error {
					for offset := 0; offset < len(uids); offset += 128 {
						batch := uids[offset:min(offset+128, len(uids))]
						touchCtx, cancel := context.WithTimeout(gctx, 2*time.Second)
						if node == m.node {
							_ = m.Recover(touchCtx, batch)
						} else {
							body, _ := json.Marshal(batch)
							_, _ = service.Cluster.RequestWithContext(touchCtx, node, touchPath, body)
						}
						cancel()
						if gctx.Err() != nil {
							return gctx.Err()
						}
					}
					return nil
				})
			}
			_ = g.Wait()
		}
	}
}

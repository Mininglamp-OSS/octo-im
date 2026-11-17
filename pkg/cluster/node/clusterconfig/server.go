package clusterconfig

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/bwmarrin/snowflake"
	"go.uber.org/zap"
)

type Server struct {
	opts     *Options
	raft     *raft.Raft // raft算法
	raftMu   sync.RWMutex
	config   *Config                // 分布式配置对象
	storage  *PebbleShardLogStorage // 配置日志存储
	cfgGenId *snowflake.Node        // 配置ID生成器
	// Apply-worker-only progress. On restart the persisted snapshot and
	// initRaft reconstruct membership; these flags cover in-process retries.
	membershipPending   bool
	configSavePending   bool
	configNotifyPending bool
	listeners           []IEvent // 事件监听器
	wklog.Log
}

func New(opts *Options) *Server {
	dataDir := path.Dir(opts.ConfigPath)

	// 是否存在dataDir,不存在则创建
	if !pathExists(dataDir) {
		err := mkDirAll(dataDir)
		if err != nil {
			panic(err)
		}
	}

	s := &Server{
		opts:   opts,
		config: NewConfig(opts),
		Log:    wklog.NewWKLog("node"),
	}

	s.storage = NewPebbleShardLogStorage(path.Join(dataDir, "cfglogdb"), s)

	var err error
	s.cfgGenId, err = snowflake.NewNode(int64(opts.NodeId))
	if err != nil {
		s.Panic("snowflake.NewNode failed", zap.Error(err))
	}

	return s
}

func pathExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil || os.IsExist(err)
}

func mkDirAll(p string) error {
	return os.MkdirAll(p, os.ModePerm)
}

func (s *Server) Start() error {

	err := s.storage.Open()
	if err != nil {
		return err
	}
	err = s.initRaft()
	if err != nil {
		return err
	}
	return s.raft.Start()
}

func (s *Server) Options() *Options {
	return s.opts
}

func (s *Server) CheckWriteHealth() error {
	if s == nil {
		return errors.New("clusterconfig server is nil")
	}
	if s.opts != nil && strings.TrimSpace(s.opts.ConfigPath) != "" {
		cfgDir := path.Dir(s.opts.ConfigPath)
		dirs := []string{
			cfgDir,
			path.Join(cfgDir, "cfglogdb"),
		}
		for _, d := range dirs {
			info, err := os.Stat(d)
			if err != nil {
				return fmt.Errorf("clusterconfig storage directory %s stat failed: %w", d, err)
			}
			if !info.IsDir() {
				return fmt.Errorf("clusterconfig storage path %s is not a directory", d)
			}
			if info.Mode().Perm()&0200 == 0 {
				return errors.New("clusterconfig storage directory is read-only: " + d)
			}
		}
	}
	if s.storage == nil {
		return errors.New("clusterconfig storage is nil")
	}
	if err := s.storage.CheckWriteHealth(); err != nil {
		return err
	}
	r := s.getRaft()
	if r == nil {
		return errors.New("clusterconfig raft not initialized")
	}
	if r.ApplyFailureCount() > 0 {
		return fmt.Errorf("clusterconfig raft has %d apply failures", r.ApplyFailureCount())
	}
	if r.StoreFailureCount() > 0 {
		return fmt.Errorf("clusterconfig raft has %d store failures", r.StoreFailureCount())
	}
	return nil
}

func (s *Server) initRaft() error {
	var raftConfig rafttypes.Config
	if s.config.isInitialized() {
		raftConfig = s.configToRaftConfig(s.config)
	} else {
		replicas := make([]uint64, 0, len(s.opts.InitNodes))
		learners := make([]uint64, 0)

		var leader uint64
		if len(s.opts.InitNodes) > 0 {
			for nodeId := range s.opts.InitNodes {
				replicas = append(replicas, nodeId)
			}
		} else {

			if strings.TrimSpace(s.opts.Seed) != "" {
				// 有种子节点，需要加入集群
				seedNodeId, _, err := seedNode(s.opts.Seed)
				if err != nil {
					return err
				}
				replicas = append(replicas, seedNodeId)    // 初次启动，如果有种子节点，种子节点作为replica
				learners = append(learners, s.opts.NodeId) // 初次启动，如果有种子节点，当前节点作为learner
			} else {
				// 单节点启动
				replicas = append(replicas, s.opts.NodeId)
				leader = s.opts.NodeId
			}

		}
		raftConfig = rafttypes.Config{
			Replicas: replicas,
			Learners: learners,
			Term:     1,
			Leader:   leader,
		}
	}

	r := raft.New(raft.NewOptions(
		raft.WithKey("clusterconfig"),
		raft.WithNodeId(s.opts.NodeId),
		raft.WithTransport(newRaftTransport(s)),
		raft.WithStorage(s.storage),
		raft.WithElectionOn(true),
	))
	r.Step(rafttypes.Event{
		Type:   rafttypes.ConfChange,
		Config: raftConfig,
	})
	s.raftMu.Lock()
	s.raft = r
	s.raftMu.Unlock()

	return nil
}

func (s *Server) getRaft() *raft.Raft {
	s.raftMu.RLock()
	defer s.raftMu.RUnlock()
	return s.raft
}

func (s *Server) Stop() {
	if r := s.getRaft(); r != nil {
		r.Stop()
	}
	s.storage.Close()
}

func (s *Server) AddEventListener(listener IEvent) {
	s.listeners = append(s.listeners, listener)
}

func (s *Server) RemoveEventListener(listener IEvent) {
	for i, l := range s.listeners {
		if l == listener {
			s.listeners = append(s.listeners[:i], s.listeners[i+1:]...)
			break
		}
	}
}

func (s *Server) NotifyConfigChangeEvent() {
	for _, l := range s.listeners {
		l.OnConfigChange(s.GetClusterConfig())
	}
}

func (s *Server) Propose(id uint64, data []byte) (*rafttypes.ProposeResp, error) {
	return s.raft.Propose(id, data)
}

func (s *Server) ProposeUntilAppliedTimeout(ctx context.Context, id uint64, data []byte) (*rafttypes.ProposeResp, error) {

	return s.raft.ProposeUntilAppliedTimeout(ctx, id, data)
}

func (s *Server) ProposeUntilApplied(id uint64, data []byte) (*rafttypes.ProposeResp, error) {
	return s.raft.ProposeUntilApplied(id, data)
}

func (s *Server) ProposeBatchTimeout(ctx context.Context, reqs []rafttypes.ProposeReq) ([]*rafttypes.ProposeResp, error) {
	return s.raft.ProposeBatchTimeout(ctx, reqs)
}

func (s *Server) ProposeBatchUntilAppliedTimeout(ctx context.Context, reqs []rafttypes.ProposeReq) ([]*rafttypes.ProposeResp, error) {
	return s.raft.ProposeBatchUntilAppliedTimeout(ctx, reqs)
}

func (s *Server) StepRaftEvent(e rafttypes.Event) {
	r := s.getRaft()
	if r == nil {
		return
	}
	r.Step(e)
}

func (s *Server) switchConfig(cfg *Config) error {
	r := s.getRaft()
	if r == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := r.StepWait(ctx, rafttypes.Event{
		Type:   rafttypes.ConfChange,
		Config: s.configToRaftConfig(cfg),
	})
	// A leader's ConfigResp can arrive before the corresponding local apply.
	// Replaying an older committed entry must not roll Raft back or stop apply.
	if errors.Is(err, raft.ErrConfigVersionStale) {
		s.Debug("raft already has newer membership", zap.Uint64("appliedVersion", cfg.version()))
		return nil
	}
	return err
}

func (s *Server) IsLeader() bool {
	r := s.getRaft()
	if r == nil {
		return false
	}
	return r.IsLeader()
}

func (s *Server) LeaderId() uint64 {
	r := s.getRaft()
	if r == nil {
		return 0
	}
	return r.LeaderId()
}

func (s *Server) GetClusterConfig() *types.Config {
	s.config.mu.RLock()
	defer s.config.mu.RUnlock()
	if s.config.cfg == nil {
		return nil
	}
	return s.config.cfg.Clone()
}

// 是否已初始化
func (s *Server) IsInitialized() bool {
	return s.config.isInitialized()
}

func (s *Server) Node(id uint64) *pb.Node {
	return s.config.node(id)
}

func (s *Server) Nodes() []*pb.Node {
	return s.config.nodes()
}
func (s *Server) Slots() []*pb.Slot {
	return s.config.slots()
}

func (s *Server) SlotCount() uint32 {
	return s.config.slotCount()
}

func (s *Server) Slot(id uint32) *pb.Slot {
	return s.config.slot(id)
}

func (s *Server) SlotLeaderId(id uint32) uint64 {
	st := s.config.slot(id)
	if st == nil {
		return 0
	}
	return st.Leader
}

// AllowVoteNodes 获取允许投票的节点
func (s *Server) AllowVoteNodes() []*pb.Node {
	return s.config.allowVoteNodes()
}

// 获取允许投票的并且已经加入了的节点集合
func (s *Server) AllowVoteAndJoinedNodes() []*pb.Node {
	return s.config.allowVoteAndJoinedNodes()
}

// 获取允许投票的并且已经加入了的节点数量
func (s *Server) AllowVoteAndJoinedNodeCount() int {
	return s.config.allowVoteAndJoinedNodeCount()
}

// 获取允许投票的并且已经加入了的在线节点数量
func (s *Server) AllowVoteAndJoinedOnlineNodeCount() int {
	return s.config.allowVoteAndJoinedOnlineNodeCount()
}

// 获取允许投票的并且已经加入了的在线节点
func (s *Server) AllowVoteAndJoinedOnlineNodes() []*pb.Node {

	return s.config.allowVoteAndJoinedOnlineNodes()
}

// 节点是否在线
func (s *Server) NodeIsOnline(nodeId uint64) bool {
	return s.config.nodeOnline(nodeId)
}

// 获取槽的副本数量
func (s *Server) SlotReplicaCount() uint32 {
	return s.config.slotReplicaCount()
}

// NodeConfigVersionFromLeader 获取节点的配置版本（只有主节点才有这个信息）
func (s *Server) NodeConfigVersionFromLeader(nodeId uint64) uint64 {
	return s.raft.GetReplicaLastLogIndex(nodeId)
}

// GetLogsByLimit 分页获取日志（升序）
func (s *Server) GetLogsByLimit(startLogIndex uint64, endLogIndex uint64, limit int) ([]rafttypes.Log, error) {
	return s.storage.GetLogsByLimit(startLogIndex, endLogIndex, limit)
}

// GetLogsInReverseOrder 分页获取日志（降序）
func (s *Server) GetLogsInReverseOrder(startLogIndex uint64, endLogIndex uint64, limit int) ([]rafttypes.Log, error) {
	return s.storage.GetLogsInReverseOrder(startLogIndex, endLogIndex, limit)
}

func (s *Server) AppliedLogIndex() (uint64, error) {
	return s.storage.AppliedIndex()
}

func (s *Server) LastLogIndex() (uint64, error) {
	return s.storage.LastIndex()
}

// 生成配置ID
func (s *Server) genConfigId() uint64 {
	return uint64(s.cfgGenId.Generate().Int64())
}

func (s *Server) configToRaftConfig(cfg *Config) rafttypes.Config {

	nodes := cfg.nodes()

	replicas := make([]uint64, 0, len(nodes))
	for _, node := range nodes {
		if node.AllowVote && node.Role == pb.NodeRole_NodeRoleReplica && !wkutil.ArrayContainsUint64(cfg.cfg.Learners, node.Id) {
			replicas = append(replicas, node.Id)
			continue
		}
	}

	var leader uint64
	if len(replicas) == 1 && replicas[0] == s.opts.NodeId { // 单机模式
		leader = s.opts.NodeId
	}

	return rafttypes.Config{
		Version:     cfg.version(),
		Replicas:    replicas,
		MigrateFrom: cfg.cfg.MigrateFrom,
		MigrateTo:   cfg.cfg.MigrateTo,
		Learners:    cfg.cfg.Learners,
		Leader:      leader,
		// Term:        cfg.cfg.Term, // 不需要设置term，不设置表示使用当前term，Config的term只是应用的最新的term，不表示是日志的term
	}
}

func seedNode(seed string) (uint64, string, error) {
	seedArray := strings.Split(seed, "@")
	if len(seedArray) < 2 {
		return 0, "", errors.New("seed format error")
	}
	seedNodeIDStr := seedArray[0]
	seedAddr := seedArray[1]
	seedNodeID, err := strconv.ParseUint(seedNodeIDStr, 10, 64)
	if err != nil {
		return 0, "", err
	}
	return seedNodeID, seedAddr, nil
}

package service

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"metaServer/internal/model"
	"metaServer/internal/raft"

	"github.com/dgraph-io/badger/v3"
	hashiraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// RaftService 封装 Hashicorp Raft，提供元数据共识和 Leader 选举
type RaftService struct {
	raftNode  *hashiraft.Raft
	fsm       *raft.MetadataFSM
	config    *model.Config
	
	// Raft 网络层
	transport *hashiraft.NetworkTransport
	
	// 节点信息
	nodeID   string
	raftAddr string
	
	// Leader 通知
	leaderCh <-chan bool
}

// NewRaftService 创建 Raft 服务
func NewRaftService(
	db *badger.DB,
	config *model.Config,
	nodeID string,
	raftAddr string,
) (*RaftService, error) {
	
	log.Printf("[Raft] Initializing Raft service: nodeID=%s, addr=%s", nodeID, raftAddr)
	
	// 1. 创建 FSM
	fsm := raft.NewMetadataFSM(db, config)
	
	// 2. 配置 Raft
	raftConfig := hashiraft.DefaultConfig()
	raftConfig.LocalID = hashiraft.ServerID(nodeID)
	raftConfig.HeartbeatTimeout = 1 * time.Second
	raftConfig.ElectionTimeout = 1 * time.Second
	raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond
	raftConfig.SnapshotInterval = 120 * time.Second
	raftConfig.SnapshotThreshold = 8192
	
	// 3. 创建存储目录
	raftDir := filepath.Join(config.Database.BadgerDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create raft directory: %v", err)
	}
	
	// 4. 创建日志存储（BoltDB）
	logStore, err := raftboltdb.NewBoltStore(
		filepath.Join(raftDir, "raft-log.db"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %v", err)
	}
	
	// 5. 创建稳定存储（BoltDB）
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(raftDir, "raft-stable.db"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %v", err)
	}
	
	// 6. 创建快照存储
	snapshotStore, err := hashiraft.NewFileSnapshotStore(
		raftDir, 2, os.Stderr,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %v", err)
	}
	
	// 7. 创建网络传输层
	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft address: %v", err)
	}
	
	transport, err := hashiraft.NewTCPTransport(
		raftAddr, addr, 3, 10*time.Second, os.Stderr,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %v", err)
	}
	
	// 8. 创建 Raft 实例
	raftNode, err := hashiraft.NewRaft(
		raftConfig,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("failed to create raft node: %v", err)
	}
	
	log.Printf("[Raft] Raft node created successfully")
	
	return &RaftService{
		raftNode:  raftNode,
		fsm:       fsm,
		config:    config,
		transport: transport,
		nodeID:    nodeID,
		raftAddr:  raftAddr,
		leaderCh:  raftNode.LeaderCh(),
	}, nil
}

// Bootstrap 初始化 Raft 集群（仅第一个节点调用）
func (rs *RaftService) Bootstrap(servers []hashiraft.Server) error {
	log.Printf("[Raft] Bootstrapping cluster with %d servers", len(servers))
	
	configuration := hashiraft.Configuration{Servers: servers}
	future := rs.raftNode.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		return fmt.Errorf("bootstrap failed: %v", err)
	}
	
	log.Printf("[Raft] Cluster bootstrapped successfully")
	return nil
}

// Apply 提交操作到 Raft（替代原有的 WAL.AppendLogEntry）
// 这个方法会将操作通过 Raft 共识后应用到所有节点
func (rs *RaftService) Apply(opType raft.OperationType, data interface{}) error {
	// 检查是否为 Leader
	if !rs.IsLeader() {
		return fmt.Errorf("not leader, cannot apply")
	}
	
	// 创建 Raft 日志条目
	entry, err := raft.NewRaftLogEntry(opType, data)
	if err != nil {
		return fmt.Errorf("failed to create log entry: %v", err)
	}
	
	// 序列化
	entryBytes, err := entry.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %v", err)
	}
	
	// 提交到 Raft（会自动同步到 Followers）
	future := rs.raftNode.Apply(entryBytes, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %v", err)
	}
	
	// 检查 FSM 应用结果
	if result := future.Response(); result != nil {
		if err, ok := result.(error); ok && err != nil {
			return fmt.Errorf("FSM apply failed: %v", err)
		}
	}
	
	return nil
}

// IsLeader 检查是否为 Leader
func (rs *RaftService) IsLeader() bool {
	return rs.raftNode.State() == hashiraft.Leader
}

// GetLeader 获取当前 Leader 地址
func (rs *RaftService) GetLeader() (hashiraft.ServerAddress, hashiraft.ServerID) {
	addr, id := rs.raftNode.LeaderWithID()
	return addr, id
}

// GetLeaderAddr 获取当前 Leader 地址（字符串）
func (rs *RaftService) GetLeaderAddr() string {
	_, addr := rs.GetLeader()
	return string(addr)
}

// AddVoter 添加节点到集群
func (rs *RaftService) AddVoter(
	nodeID string,
	address string,
) error {
	log.Printf("[Raft] Adding voter: nodeID=%s, address=%s", nodeID, address)
	
	future := rs.raftNode.AddVoter(
		hashiraft.ServerID(nodeID),
		hashiraft.ServerAddress(address),
		0, 0,
	)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %v", err)
	}
	
	log.Printf("[Raft] Voter added successfully: %s", nodeID)
	return nil
}

// RemoveServer 移除节点
func (rs *RaftService) RemoveServer(nodeID string) error {
	log.Printf("[Raft] Removing server: %s", nodeID)
	
	future := rs.raftNode.RemoveServer(
		hashiraft.ServerID(nodeID), 0, 0,
	)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove server: %v", err)
	}
	
	log.Printf("[Raft] Server removed successfully: %s", nodeID)
	return nil
}

// WaitForLeader 等待 Leader 选举完成
func (rs *RaftService) WaitForLeader(timeout time.Duration) error {
	log.Printf("[Raft] Waiting for leader election (timeout=%v)", timeout)
	
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	
	for {
		select {
		case <-ticker.C:
			if rs.IsLeader() {
				log.Printf("[Raft] This node became leader")
				return nil
			}
			leaderAddr := rs.GetLeaderAddr()
			if leaderAddr != "" {
				log.Printf("[Raft] Leader elected: %s", leaderAddr)
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout waiting for leader")
		}
	}
}

// GetState 获取 Raft 状态
func (rs *RaftService) GetState() hashiraft.RaftState {
	return rs.raftNode.State()
}

// GetStats 获取 Raft 统计信息
func (rs *RaftService) GetStats() map[string]string {
	return rs.raftNode.Stats()
}

// Shutdown 关闭 Raft 服务
func (rs *RaftService) Shutdown() error {
	log.Printf("[Raft] Shutting down Raft service")
	
	future := rs.raftNode.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to shutdown raft: %v", err)
	}
	
	if rs.transport != nil {
		rs.transport.Close()
	}
	
	log.Printf("[Raft] Raft service shutdown complete")
	return nil
}

// LeaderCh 返回 Leader 变化通知 channel
func (rs *RaftService) LeaderCh() <-chan bool {
	return rs.leaderCh
}

// Snapshot 手动触发快照
func (rs *RaftService) Snapshot() error {
	log.Printf("[Raft] Triggering manual snapshot")
	
	future := rs.raftNode.Snapshot()
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}
	
	log.Printf("[Raft] Snapshot created successfully")
	return nil
}

// GetConfiguration 获取集群配置
func (rs *RaftService) GetConfiguration() (hashiraft.Configuration, error) {
	future := rs.raftNode.GetConfiguration()
	if err := future.Error(); err != nil {
		return hashiraft.Configuration{}, err
	}
	return future.Configuration(), nil
}

// VerifyLeader 验证当前节点是否仍是 Leader
func (rs *RaftService) VerifyLeader() error {
	future := rs.raftNode.VerifyLeader()
	return future.Error()
}

// GetPeers 获取集群中的所有节点
func (rs *RaftService) GetPeers() ([]string, error) {
	config, err := rs.GetConfiguration()
	if err != nil {
		return nil, err
	}
	
	var peers []string
	for _, server := range config.Servers {
		peers = append(peers, string(server.Address))
	}
	
	return peers, nil
}

// MarshalJSON 实现 JSON 序列化（用于调试）
func (rs *RaftService) MarshalJSON() ([]byte, error) {
	stats := rs.GetStats()
	info := map[string]interface{}{
		"node_id":   rs.nodeID,
		"addr":      rs.raftAddr,
		"is_leader": rs.IsLeader(),
		"state":     rs.GetState().String(),
		"stats":     stats,
	}
	
	leaderID, leaderAddr := rs.GetLeader()
	if leaderAddr != "" {
		info["leader_id"] = string(leaderID)
		info["leader_addr"] = string(leaderAddr)
	}
	
	return json.Marshal(info)
}

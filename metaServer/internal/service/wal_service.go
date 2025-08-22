package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"metaServer/internal/model"
	"metaServer/pb"

	"github.com/dgraph-io/badger/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// WALService 管理Write-Ahead Log
type WALService struct {
	db            *badger.DB
	config        *model.Config
	nextLogIndex  uint64
	mutex         sync.RWMutex
	walDir        string
	
	// 用于同步到followers
	followers     map[string]*FollowerClient
	followerMutex sync.RWMutex
	
	// Leader选举引用
	leaderElection *LeaderElection
}

// FollowerClient 表示一个follower连接
type FollowerClient struct {
	nodeID   string
	address  string
	client   pb.MetaServerServiceClient
	conn     *grpc.ClientConn // gRPC连接
	lastSync uint64           // 最后同步的日志索引
}

// NewWALService 创建WAL服务
func NewWALService(db *badger.DB, config *model.Config, nodeID string) *WALService {
	walDir := filepath.Join(config.Database.BadgerDir, "wal")
	os.MkdirAll(walDir, 0755)
	
	ws := &WALService{
		db:        db,
		config:    config,
		walDir:    walDir,
		followers: make(map[string]*FollowerClient),
	}
	
	// 初始化下一个日志索引
	ws.initNextLogIndex()
	
	log.Printf("WAL Service initialized for node %s, next log index: %d", nodeID, ws.nextLogIndex)
	return ws
}

// initNextLogIndex 初始化下一个日志索引
func (ws *WALService) initNextLogIndex() {
	err := ws.db.View(func(txn *badger.Txn) error {
		// 查找最大的日志索引
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		
		prefix := []byte("wal:")
		maxIndex := uint64(0)
		
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var entry pb.LogEntry
				if err := proto.Unmarshal(val, &entry); err == nil {
					if entry.LogIndex > maxIndex {
						maxIndex = entry.LogIndex
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		
		ws.nextLogIndex = maxIndex + 1
		return nil
	})
	
	if err != nil {
		log.Printf("Failed to initialize next log index: %v", err)
		ws.nextLogIndex = 1
	}
}

// AppendLogEntry 追加一个日志条目
func (ws *WALService) AppendLogEntry(operation pb.WALOperationType, data interface{}) (*pb.LogEntry, error) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	
	// 序列化操作数据
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal operation data: %v", err)
	}
	
	// 计算校验和
	hash := sha256.Sum256(jsonData)
	checksum := hex.EncodeToString(hash[:])
	
	// 创建日志条目
	entry := &pb.LogEntry{
		LogIndex:  ws.nextLogIndex,
		Timestamp: time.Now().UnixMilli(),
		Operation: operation,
		Data:      jsonData,
		Checksum:  checksum,
	}
	
	// 写入到BadgerDB
	err = ws.writeLogEntry(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to write log entry: %v", err)
	}
	
	// 增加日志索引
	ws.nextLogIndex++
	
	log.Printf("WAL: Appended log entry %d, operation: %v", entry.LogIndex, operation)
	return entry, nil
}

// WriteLogEntry 将日志条目写入BadgerDB（公共方法，供SyncWAL使用）
func (ws *WALService) WriteLogEntry(entry *pb.LogEntry) error {
	return ws.writeLogEntry(entry)
}

// writeLogEntry 将日志条目写入BadgerDB
func (ws *WALService) writeLogEntry(entry *pb.LogEntry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	
	key := fmt.Sprintf("wal:%010d", entry.LogIndex)
	return ws.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// GetLogEntry 获取指定索引的日志条目
func (ws *WALService) GetLogEntry(logIndex uint64) (*pb.LogEntry, error) {
	var entry pb.LogEntry
	
	err := ws.db.View(func(txn *badger.Txn) error {
		key := fmt.Sprintf("wal:%010d", logIndex)
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		
		return item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &entry)
		})
	})
	
	if err != nil {
		return nil, err
	}
	
	return &entry, nil
}

// GetLogEntriesFrom 获取从指定索引开始的日志条目
func (ws *WALService) GetLogEntriesFrom(startIndex uint64, limit int) ([]*pb.LogEntry, error) {
	var entries []*pb.LogEntry
	
	err := ws.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		
		startKey := fmt.Sprintf("wal:%010d", startIndex)
		count := 0
		
		for it.Seek([]byte(startKey)); it.Valid() && count < limit; it.Next() {
			item := it.Item()
			
			err := item.Value(func(val []byte) error {
				var entry pb.LogEntry
				if err := proto.Unmarshal(val, &entry); err != nil {
					return err
				}
				entries = append(entries, &entry)
				count++
				return nil
			})
			
			if err != nil {
				return err
			}
		}
		
		return nil
	})
	
	return entries, err
}

// ReplayLogEntry 回放一个日志条目
func (ws *WALService) ReplayLogEntry(entry *pb.LogEntry, metadataService *MetadataService) error {
	// 验证校验和
	hash := sha256.Sum256(entry.Data)
	expectedChecksum := hex.EncodeToString(hash[:])
	if entry.Checksum != expectedChecksum {
		return fmt.Errorf("checksum mismatch for log entry %d", entry.LogIndex)
	}
	
	// 根据操作类型回放
	switch entry.Operation {
	case pb.WALOperationType_CREATE_NODE:
		var op pb.CreateNodeOperation
		if err := json.Unmarshal(entry.Data, &op); err != nil {
			return fmt.Errorf("failed to unmarshal CreateNodeOperation: %v", err)
		}
		
		log.Printf("WAL Replay: CreateNode %s (type: %v, expected inode: %d)", op.Path, op.Type, op.InodeId)
		
		// 检查文件是否已存在
		existingNodeInfo, err := metadataService.GetNodeInfo(op.Path)
		if err == nil {
			// 文件已存在，检查inode ID是否一致
			if existingNodeInfo.Inode == op.InodeId {
				log.Printf("WAL Replay: CreateNode %s already exists with correct inode %d, skipping", op.Path, op.InodeId)
				return nil
			} else {
				log.Printf("WAL Replay: CreateNode %s already exists but with different inode %d (expected %d), this indicates data inconsistency", 
					op.Path, existingNodeInfo.Inode, op.InodeId)
				return fmt.Errorf("inode mismatch for path %s: existing=%d, expected=%d", op.Path, existingNodeInfo.Inode, op.InodeId)
			}
		}
		
		// 文件不存在，使用指定的 Inode ID 创建
		err = metadataService.CreateNodeWithInode(op.Path, op.Type, &op.InodeId)
		if err != nil {
			log.Printf("WAL Replay: Failed to create node %s with inode %d: %v", op.Path, op.InodeId, err)
			return err
		}
		
		log.Printf("WAL Replay: Successfully created node %s with inode %d", op.Path, op.InodeId)
		return nil
		
	case pb.WALOperationType_DELETE_NODE:
		var op pb.DeleteNodeOperation
		if err := json.Unmarshal(entry.Data, &op); err != nil {
			return fmt.Errorf("failed to unmarshal DeleteNodeOperation: %v", err)
		}
		
		log.Printf("WAL Replay: DeleteNode %s (recursive: %v)", op.Path, op.Recursive)
		
		// 执行删除操作
		_, err := metadataService.DeleteNode(op.Path, op.Recursive)
		if err != nil {
			log.Printf("WAL Replay: Failed to delete node %s: %v", op.Path, err)
			return err
		}
		
		log.Printf("WAL Replay: Successfully deleted node %s", op.Path)
		return nil
		
	case pb.WALOperationType_UPDATE_NODE:
		var op pb.UpdateNodeOperation
		if err := json.Unmarshal(entry.Data, &op); err != nil {
			return fmt.Errorf("failed to unmarshal UpdateNodeOperation: %v", err)
		}
		// 这里需要在MetadataService中添加UpdateNode方法
		log.Printf("WAL Replay: UpdateNode %s (size: %d, mtime: %d)", op.Path, op.Size, op.Mtime)
		return nil
		
	case pb.WALOperationType_FINALIZE_WRITE:
		var op pb.FinalizeWriteOperation
		if err := json.Unmarshal(entry.Data, &op); err != nil {
			return fmt.Errorf("failed to unmarshal FinalizeWriteOperation: %v", err)
		}
		
		log.Printf("WAL Replay: FinalizeWrite %s (inode=%d, size=%d, md5=%s, %d blocks)", 
			op.Path, op.Inode, op.Size, op.Md5, len(op.BlockLocations))
		
		// 检查文件是否存在
		nodeInfo, err := metadataService.GetNodeInfo(op.Path)
		if err != nil {
			// 文件不存在，这不应该发生，因为应该先有CreateNode
			log.Printf("WAL Replay: ERROR - FinalizeWrite for non-existent file %s (expected inode %d)", op.Path, op.Inode)
			return fmt.Errorf("cannot finalize write for non-existent file: %s", op.Path)
		}
		
		// 验证 Inode ID 是否一致
		if nodeInfo.Inode != op.Inode {
			log.Printf("WAL Replay: ERROR - Inode mismatch for FinalizeWrite %s: existing=%d, expected=%d", 
				op.Path, nodeInfo.Inode, op.Inode)
			return fmt.Errorf("inode mismatch for FinalizeWrite %s: existing=%d, expected=%d", 
				op.Path, nodeInfo.Inode, op.Inode)
		}
		
		// 设置块映射
		log.Printf("WAL Replay: Setting block mappings for inode %d", op.Inode)
		for i, blockMapping := range op.BlockLocations {
			err := metadataService.setBlockMappingInDB(op.Inode, uint64(i), blockMapping)
			if err != nil {
				log.Printf("WAL Replay: Failed to set block mapping for %s (inode %d): %v", op.Path, op.Inode, err)
				return err
			}
		}
		
		// 执行FinalizeWrite操作
		log.Printf("WAL Replay: Finalizing write for %s with inode %d", op.Path, op.Inode)
		err = metadataService.FinalizeWrite(op.Path, op.Inode, uint64(op.Size), op.Md5)
		if err != nil {
			log.Printf("WAL Replay: Failed to finalize write for %s: %v", op.Path, err)
			return err
		}
		
		log.Printf("WAL Replay: Successfully replayed FinalizeWrite %s", op.Path)
		return nil
		
	case pb.WALOperationType_UPDATE_BLOCK_LOCATION:
		var op pb.UpdateBlockLocationOperation
		if err := json.Unmarshal(entry.Data, &op); err != nil {
			return fmt.Errorf("failed to unmarshal UpdateBlockLocationOperation: %v", err)
		}
		
		log.Printf("WAL Replay: UpdateBlockLocation block=%d from %s to %s", 
			op.BlockId, op.OldAddr, op.NewAddr)
		
		// 执行块位置更新操作（直接调用数据库方法，避免再次写WAL）
		err := metadataService.updateBlockLocationInDB(op.BlockId, op.OldAddr, op.NewAddr)
		if err != nil {
			log.Printf("WAL Replay: Failed to update block location for block %d: %v", op.BlockId, err)
			return err
		}
		
		log.Printf("WAL Replay: Successfully updated block %d location from %s to %s", 
			op.BlockId, op.OldAddr, op.NewAddr)
		return nil
		
	case pb.WALOperationType_SET_BLOCK_MAPPING:
		var op pb.SetBlockMappingOperation
		if err := json.Unmarshal(entry.Data, &op); err != nil {
			return fmt.Errorf("failed to unmarshal SetBlockMappingOperation: %v", err)
		}
		
		log.Printf("WAL Replay: SetBlockMapping inode=%d, index=%d", op.InodeId, op.BlockIndex)
		
		// 执行块映射设置操作（直接调用数据库方法，避免再次写WAL）
		err := metadataService.setBlockMappingInDB(op.InodeId, op.BlockIndex, op.BlockLocs)
		if err != nil {
			log.Printf("WAL Replay: Failed to set block mapping for inode %d index %d: %v", 
				op.InodeId, op.BlockIndex, err)
			return err
		}
		
		log.Printf("WAL Replay: Successfully set block mapping for inode %d index %d", 
			op.InodeId, op.BlockIndex)
		return nil
		
	default:
		return fmt.Errorf("unknown WAL operation type: %v", entry.Operation)
	}
}

// AddFollower 添加一个follower
func (ws *WALService) AddFollower(nodeID, address string, client pb.MetaServerServiceClient) {
	ws.followerMutex.Lock()
	defer ws.followerMutex.Unlock()
	
	ws.followers[nodeID] = &FollowerClient{
		nodeID:   nodeID,
		address:  address,
		client:   client,
		lastSync: 0,
	}
	
	log.Printf("WAL: Added follower %s at %s", nodeID, address)
}

// AddFollowerByAddr 通过地址添加follower（创建gRPC连接）
func (ws *WALService) AddFollowerByAddr(nodeID, address string) error {
	ws.followerMutex.Lock()
	defer ws.followerMutex.Unlock()
	
	// 创建gRPC连接
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to follower %s at %s: %v", nodeID, address, err)
	}
	
	client := pb.NewMetaServerServiceClient(conn)
	
	// 如果已存在，先关闭旧连接
	if existingFollower, exists := ws.followers[nodeID]; exists && existingFollower.conn != nil {
		existingFollower.conn.Close()
	}
	
	ws.followers[nodeID] = &FollowerClient{
		nodeID:   nodeID,
		address:  address,
		client:   client,
		conn:     conn,
		lastSync: 0,
	}
	
	log.Printf("WAL: Added follower %s at %s with gRPC connection", nodeID, address)
	return nil
}

// RemoveFollower 移除一个follower
func (ws *WALService) RemoveFollower(nodeID string) {
	ws.followerMutex.Lock()
	defer ws.followerMutex.Unlock()
	
	if follower, exists := ws.followers[nodeID]; exists && follower.conn != nil {
		follower.conn.Close()
	}
	
	delete(ws.followers, nodeID)
	log.Printf("WAL: Removed follower %s", nodeID)
}

// UpdateFollowers 更新followers列表（leader选举时调用）
func (ws *WALService) UpdateFollowers(followerNodes []*MetaServerNode) {
	ws.followerMutex.Lock()
	defer ws.followerMutex.Unlock()
	
	// 关闭所有现有连接
	for _, follower := range ws.followers {
		if follower.conn != nil {
			follower.conn.Close()
		}
	}
	
	// 清空followers
	ws.followers = make(map[string]*FollowerClient)
	
	// 添加新的followers
	for _, node := range followerNodes {
		conn, err := grpc.Dial(node.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("WAL: Failed to connect to follower %s at %s: %v", node.NodeID, node.Addr, err)
			continue
		}
		
		client := pb.NewMetaServerServiceClient(conn)
		
		ws.followers[node.NodeID] = &FollowerClient{
			nodeID:   node.NodeID,
			address:  node.Addr,
			client:   client,
			conn:     conn,
			lastSync: 0,
		}
		
		log.Printf("WAL: Connected to follower %s at %s", node.NodeID, node.Addr)
	}
	
	log.Printf("WAL: Updated followers list, now have %d followers", len(ws.followers))
}

// SyncToFollowers 同步日志到所有followers
func (ws *WALService) SyncToFollowers(entry *pb.LogEntry) error {
	ws.followerMutex.RLock()
	followers := make(map[string]*FollowerClient)
	for k, v := range ws.followers {
		followers[k] = v
	}
	ws.followerMutex.RUnlock()
	
	// 并行同步到所有followers
	var wg sync.WaitGroup
	errors := make(chan error, len(followers))
	
	for _, follower := range followers {
		wg.Add(1)
		go func(f *FollowerClient) {
			defer wg.Done()
			
			// 调用follower的SyncWAL接口发送WAL条目
			err := ws.syncWALToFollower(f, entry)
			if err != nil {
				log.Printf("Failed to sync WAL entry %d to follower %s: %v", entry.LogIndex, f.nodeID, err)
				errors <- err
			} else {
				log.Printf("WAL: Successfully synced entry %d to follower %s", entry.LogIndex, f.nodeID)
				f.lastSync = entry.LogIndex
			}
		}(follower)
	}
	
	wg.Wait()
	close(errors)
	
	// 检查是否有错误
	for err := range errors {
		if err != nil {
			return fmt.Errorf("failed to sync to some followers: %v", err)
		}
	}
	
	return nil
}

// syncWALToFollower 向单个follower同步WAL条目
func (ws *WALService) syncWALToFollower(follower *FollowerClient, entry *pb.LogEntry) error {
	if follower.client == nil {
		return fmt.Errorf("follower %s has no gRPC client", follower.nodeID)
	}
	
	// 创建gRPC流
	stream, err := follower.client.SyncWAL(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create SyncWAL stream: %v", err)
	}
	
	// 发送WAL条目
	err = stream.Send(entry)
	if err != nil {
		return fmt.Errorf("failed to send WAL entry: %v", err)
	}
	
	// 关闭流并获取响应
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close stream: %v", err)
	}
	
	if !resp.Success {
		return fmt.Errorf("follower rejected WAL entry: %s", resp.Message)
	}
	
	return nil
}

// GetCurrentLogIndex 获取当前日志索引
func (ws *WALService) GetCurrentLogIndex() uint64 {
	ws.mutex.RLock()
	defer ws.mutex.RUnlock()
	return ws.nextLogIndex - 1
}

// UpdateNextLogIndex 更新下一个日志索引（用于同步时）
func (ws *WALService) UpdateNextLogIndex(nextIndex uint64) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	if nextIndex > ws.nextLogIndex {
		ws.nextLogIndex = nextIndex
	}
}

// Close 关闭WAL服务
func (ws *WALService) Close() error {
	ws.followerMutex.Lock()
	defer ws.followerMutex.Unlock()
	
	// 关闭所有follower连接
	for _, follower := range ws.followers {
		if follower.conn != nil {
			follower.conn.Close()
		}
	}
	
	log.Println("WAL Service stopped")
	return nil
}

// SetLeaderElection 设置Leader选举引用
func (ws *WALService) SetLeaderElection(le *LeaderElection) {
	ws.leaderElection = le
}

// IsLeader 检查当前节点是否是Leader
func (ws *WALService) IsLeader() bool {
	if ws.leaderElection == nil {
		return false
	}
	return ws.leaderElection.IsLeader()
}

// RequestWALSyncFromLeader 从leader请求WAL同步（当节点重新加入集群时）
func (ws *WALService) RequestWALSyncFromLeader(leaderAddr string, nodeID string, lastLogIndex uint64) error {
	log.Printf("Requesting WAL sync from leader %s, node %s, from index %d", leaderAddr, nodeID, lastLogIndex)
	
	// 创建连接到leader的gRPC客户端
	conn, err := grpc.Dial(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to leader %s: %v", leaderAddr, err)
	}
	defer conn.Close()
	
	client := pb.NewMetaServerServiceClient(conn)
	
	// 发送WAL同步请求
	req := &pb.RequestWALSyncRequest{
		NodeId:       nodeID,
		LastLogIndex: lastLogIndex,
		Reason:       "rejoin_cluster",
	}
	
	stream, err := client.RequestWALSync(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to request WAL sync: %v", err)
	}
	
	syncedCount := 0
	fullReset := false
	
	// 接收WAL条目流
	for {
		entry, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("WAL sync completed from leader, synced %d entries (full reset: %v)", syncedCount, fullReset)
				break
			}
			return fmt.Errorf("failed to receive WAL entry: %v", err)
		}
		
		// 检查是否需要完全重置（第一个条目的索引为1表示需要完全重置）
		if syncedCount == 0 && entry.LogIndex == 1 {
			log.Printf("Detected log divergence, performing full WAL reset")
			fullReset = true
			
			// 清空现有WAL数据
			err = ws.clearAllWALEntries()
			if err != nil {
				return fmt.Errorf("failed to clear WAL entries: %v", err)
			}
			
			// 重置日志索引
			ws.mutex.Lock()
			ws.nextLogIndex = 1
			ws.mutex.Unlock()
			
			log.Printf("WAL database cleared, ready for complete resync")
		}
		
		// 将WAL条目写入本地存储但不执行
		err = ws.WriteLogEntry(entry)
		if err != nil {
			return fmt.Errorf("failed to write WAL entry %d: %v", entry.LogIndex, err)
		}
		
		// 更新下一个日志索引
		ws.mutex.Lock()
		if entry.LogIndex >= ws.nextLogIndex {
			ws.nextLogIndex = entry.LogIndex + 1
		}
		ws.mutex.Unlock()
		
		syncedCount++
		log.Printf("Synced WAL entry %d from leader, operation: %v", entry.LogIndex, entry.Operation)
	}
	
	log.Printf("WAL sync from leader completed successfully, total entries: %d (full reset: %v)", syncedCount, fullReset)
	return nil
}

// GetAllLogEntries 获取所有WAL日志条目（用于leader响应同步请求）
func (ws *WALService) GetAllLogEntries(fromIndex uint64) ([]*pb.LogEntry, error) {
	var entries []*pb.LogEntry
	
	// 首先检查是否存在日志分叉
	leaderMaxIndex := ws.GetCurrentLogIndex()
	if fromIndex > leaderMaxIndex {
		log.Printf("Log divergence detected: follower index %d > leader index %d, returning full WAL", fromIndex, leaderMaxIndex)
		// 日志分叉：follower的索引比leader还大，需要完全重置
		// 返回从索引1开始的所有条目
		fromIndex = 1
	}
	
	err := ws.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		
		startKey := fmt.Sprintf("wal:%010d", fromIndex)
		prefix := []byte("wal:")
		
		for it.Seek([]byte(startKey)); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			
			err := item.Value(func(val []byte) error {
				var entry pb.LogEntry
				if err := proto.Unmarshal(val, &entry); err != nil {
					return err
				}
				entries = append(entries, &entry)
				return nil
			})
			
			if err != nil {
				return err
			}
		}
		
		return nil
	})
	
	if err != nil {
		return nil, err
	}
	
	if fromIndex == 1 && len(entries) > 0 {
		log.Printf("Returning FULL WAL reset: %d entries starting from index 1 (divergence fix)", len(entries))
	} else {
		log.Printf("Retrieved %d WAL entries starting from index %d (incremental sync)", len(entries), fromIndex)
	}
	return entries, nil
}

// ReplayWALFromIndex 从指定索引开始回放WAL（仅在当选leader时使用）
func (ws *WALService) ReplayWALFromIndex(startIndex uint64, metadataService *MetadataService) error {
	if !ws.IsLeader() {
		return fmt.Errorf("only leader can replay WAL")
	}
	
	log.Printf("Starting WAL replay from index %d", startIndex)
	
	entries, err := ws.GetAllLogEntries(startIndex)
	if err != nil {
		return fmt.Errorf("failed to get WAL entries: %v", err)
	}
	
	replayedCount := 0
	for _, entry := range entries {
		err = ws.ReplayLogEntry(entry, metadataService)
		if err != nil {
			log.Printf("Failed to replay WAL entry %d: %v", entry.LogIndex, err)
			return err
		}
		replayedCount++
	}
	
	log.Printf("WAL replay completed, replayed %d entries", replayedCount)
	return nil
}

// clearAllWALEntries 清空所有WAL条目（用于分叉恢复时的强制重置）
func (ws *WALService) clearAllWALEntries() error {
	log.Printf("Clearing all WAL entries due to log divergence")
	
	// 收集所有WAL keys
	var keysToDelete []string
	
	err := ws.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		
		prefix := []byte("wal:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			keysToDelete = append(keysToDelete, key)
		}
		
		return nil
	})
	
	if err != nil {
		return fmt.Errorf("failed to collect WAL keys: %v", err)
	}
	
	// 删除所有WAL keys
	err = ws.db.Update(func(txn *badger.Txn) error {
		for _, key := range keysToDelete {
			err := txn.Delete([]byte(key))
			if err != nil {
				return fmt.Errorf("failed to delete WAL key %s: %v", key, err)
			}
		}
		return nil
	})
	
	if err != nil {
		return fmt.Errorf("failed to delete WAL entries: %v", err)
	}
	
	log.Printf("Successfully cleared %d WAL entries", len(keysToDelete))
	return nil
}
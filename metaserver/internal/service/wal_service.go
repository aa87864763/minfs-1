package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"metaserver/internal/model"
	"metaserver/pb"

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
		return metadataService.CreateNode(op.Path, op.Type)
		
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
		
		// 检查文件是否存在，如果不存在则先创建基本文件元数据
		_, err := metadataService.GetNodeInfo(op.Path)
		if err != nil {
			// 文件不存在，先创建基本文件元数据
			log.Printf("WAL Replay: File %s does not exist, creating basic file metadata first", op.Path)
			err = ws.createFileForReplay(metadataService, op.Path, op.Inode)
			if err != nil {
				log.Printf("WAL Replay: Failed to create file metadata for %s: %v", op.Path, err)
				return err
			}
		}
		
		// 先恢复块映射
		for i, blockMapping := range op.BlockLocations {
			err := metadataService.SetBlockMapping(op.Inode, uint64(i), blockMapping)
			if err != nil {
				log.Printf("WAL Replay: Failed to set block mapping for %s: %v", op.Path, err)
				return err
			}
		}
		
		// 执行FinalizeWrite操作
		err = metadataService.FinalizeWrite(op.Path, op.Inode, uint64(op.Size), op.Md5)
		if err != nil {
			log.Printf("WAL Replay: Failed to finalize write for %s: %v", op.Path, err)
			return err
		}
		
		log.Printf("WAL Replay: Successfully replayed FinalizeWrite %s", op.Path)
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

// createFileForReplay 在WAL回放时创建文件的基本元数据
func (ws *WALService) createFileForReplay(metadataService *MetadataService, path string, inodeID uint64) error {
	// 导入必要的包
	// 这个方法模仿CreateNode但使用指定的inodeID
	path = filepath.Clean(path)
	if path == "." {
		path = "/"
	}
	
	return metadataService.db.Update(func(txn *badger.Txn) error {
		// 检查路径是否已存在
		pathKey := "path:" + path
		_, err := txn.Get([]byte(pathKey))
		if err == nil {
			// 路径已存在，不需要创建
			return nil
		}
		if err != badger.ErrKeyNotFound {
			return err
		}
		
		// 检查父目录是否存在
		parentPath := filepath.Dir(path)
		if parentPath != "/" {
			parentKey := "path:" + parentPath
			_, err := txn.Get([]byte(parentKey))
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("parent directory does not exist: %s", parentPath)
			}
			if err != nil {
				return err
			}
		}
		
		// 创建 NodeInfo 使用指定的inodeID
		nodeInfo := &pb.NodeInfo{
			Inode:       inodeID,
			Path:        path,
			Type:        pb.FileType_File,
			Size:        0,
			Mtime:       time.Now().UnixMilli(),
			Replication: 3, // 默认副本数
		}
		
		// 序列化并存储 Inode 信息
		data, err := proto.Marshal(nodeInfo)
		if err != nil {
			return err
		}
		
		inodeKey := fmt.Sprintf("inode:%d", inodeID)
		if err := txn.Set([]byte(inodeKey), data); err != nil {
			return err
		}
		
		// 存储路径映射
		inodeBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(inodeBuf, inodeID)
		if err := txn.Set([]byte(pathKey), inodeBuf); err != nil {
			return err
		}
		
		// 如果不是根目录，更新父目录的目录条目
		if path != "/" {
			parentPath := filepath.Dir(path)
			fileName := filepath.Base(path)
			
			// 获取父目录的 Inode ID
			parentKey := "path:" + parentPath
			item, err := txn.Get([]byte(parentKey))
			if err != nil {
				return err
			}
			
			var parentInodeID uint64
			err = item.Value(func(val []byte) error {
				parentInodeID = binary.BigEndian.Uint64(val)
				return nil
			})
			if err != nil {
				return err
			}
			
			// 添加目录条目
			dirKey := fmt.Sprintf("dir:%d/%s", parentInodeID, fileName)
			if err := txn.Set([]byte(dirKey), inodeBuf); err != nil {
				return err
			}
		}
		
		return nil
	})
}
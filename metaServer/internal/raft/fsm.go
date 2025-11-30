package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"time"

	"metaServer/internal/model"
	"metaServer/pb"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// MetadataFSM 实现 raft.FSM 接口
// 封装 BadgerDB 操作，确保所有元数据变更通过 Raft 共识
type MetadataFSM struct {
	db     *badger.DB
	config *model.Config
}

// NewMetadataFSM 创建 FSM 实例
func NewMetadataFSM(db *badger.DB, config *model.Config) *MetadataFSM {
	return &MetadataFSM{
		db:     db,
		config: config,
	}
}

// Apply 应用日志到状态机（核心方法，Raft 调用）
// 这个方法在所有节点上执行，确保状态一致
func (fsm *MetadataFSM) Apply(logEntry *raft.Log) interface{} {
	// 1. 反序列化日志条目
	entry, err := UnmarshalRaftLogEntry(logEntry.Data)
	if err != nil {
		log.Printf("[FSM] Failed to unmarshal raft log entry: %v", err)
		return err
	}
	
	log.Printf("[FSM] Applying log index=%d, type=%s", logEntry.Index, entry.Type)
	
	// 2. 根据操作类型分发处理
	switch entry.Type {
	case OpCreateNode:
		return fsm.applyCreateNode(entry.Data)
	case OpDeleteNode:
		return fsm.applyDeleteNode(entry.Data)
	case OpFinalizeWrite:
		return fsm.applyFinalizeWrite(entry.Data)
	case OpSetBlockMapping:
		return fsm.applySetBlockMapping(entry.Data)
	case OpUpdateBlockLocation:
		return fsm.applyUpdateBlockLocation(entry.Data)
	default:
		err := fmt.Errorf("unknown operation type: %v", entry.Type)
		log.Printf("[FSM] %v", err)
		return err
	}
}

// applyCreateNode 应用创建节点操作
func (fsm *MetadataFSM) applyCreateNode(data json.RawMessage) error {
	var op CreateNodeOperation
	if err := json.Unmarshal(data, &op); err != nil {
		return fmt.Errorf("unmarshal CreateNodeOperation: %w", err)
	}
	
	log.Printf("[FSM] Creating node: path=%s, type=%s, inode=%d", op.Path, op.NodeType, op.InodeID)
	
	// 标准化路径
	path := filepath.Clean(op.Path)
	if path == "." {
		path = "/"
	}
	
	// 转换 NodeType
	var fileType pb.FileType
	if op.NodeType == "DIRECTORY" {
		fileType = pb.FileType_Directory
	} else {
		fileType = pb.FileType_File
	}
	
	return fsm.db.Update(func(txn *badger.Txn) error {
		// 检查路径是否已存在
		pathKey := model.PrefixPath + path
		_, err := txn.Get([]byte(pathKey))
		if err == nil {
			// 路径已存在，检查 inode 是否一致
			var existingInodeID uint64
			item, _ := txn.Get([]byte(pathKey))
			item.Value(func(val []byte) error {
				existingInodeID = binary.BigEndian.Uint64(val)
				return nil
			})
			
			if existingInodeID == op.InodeID {
				log.Printf("[FSM] Node already exists with correct inode, skipping: %s", path)
				return nil
			}
			return fmt.Errorf("path exists with different inode: %s", path)
		}
		if err != badger.ErrKeyNotFound {
			return err
		}
		
		// 检查父目录
		if path != "/" {
			parentPath := filepath.Dir(path)
			if parentPath != "/" && parentPath != path {
				parentKey := model.PrefixPath + parentPath
				_, err := txn.Get([]byte(parentKey))
				if err != nil {
					return fmt.Errorf("parent directory does not exist: %s", parentPath)
				}
			}
		}
		
		// 确保 inode 计数器大于当前 inode
		if err := fsm.updateInodeCounterInTx(txn, op.InodeID); err != nil {
			return err
		}
		
		// 创建 NodeInfo
		nodeInfo := &pb.NodeInfo{
			Inode:       op.InodeID,
			Path:        path,
			Type:        fileType,
			Size:        0,
			Mtime:       time.Now().UnixMilli(),
			Replication: uint32(fsm.config.Cluster.DefaultReplication),
		}
		
		// 存储 Inode 信息
		inodeData, err := proto.Marshal(nodeInfo)
		if err != nil {
			return err
		}
		
		inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, op.InodeID)
		if err := txn.Set([]byte(inodeKey), inodeData); err != nil {
			return err
		}
		
		// 存储路径映射
		inodeBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(inodeBuf, op.InodeID)
		if err := txn.Set([]byte(pathKey), inodeBuf); err != nil {
			return err
		}
		
		// 更新父目录的目录条目
		if path != "/" {
			parentPath := filepath.Dir(path)
			fileName := filepath.Base(path)
			
			// 获取父目录的 Inode ID
			parentKey := model.PrefixPath + parentPath
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
			dirKey := fmt.Sprintf("%s%d/%s", model.PrefixDir, parentInodeID, fileName)
			if err := txn.Set([]byte(dirKey), inodeBuf); err != nil {
				return err
			}
		}
		
		log.Printf("[FSM] Node created successfully: %s (inode=%d)", path, op.InodeID)
		return nil
	})
}

// applyDeleteNode 应用删除节点操作
func (fsm *MetadataFSM) applyDeleteNode(data json.RawMessage) error {
	var op DeleteNodeOperation
	if err := json.Unmarshal(data, &op); err != nil {
		return err
	}
	
	log.Printf("[FSM] Deleting node: path=%s, recursive=%v", op.Path, op.Recursive)
	
	path := filepath.Clean(op.Path)
	if path == "." {
		path = "/"
	}
	
	if path == "/" {
		return fmt.Errorf("cannot delete root directory")
	}
	
	return fsm.db.Update(func(txn *badger.Txn) error {
		// 获取节点的 inode
		pathKey := model.PrefixPath + path
		item, err := txn.Get([]byte(pathKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				log.Printf("[FSM] Node already deleted: %s", path)
				return nil
			}
			return err
		}
		
		var inodeID uint64
		err = item.Value(func(val []byte) error {
			inodeID = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return err
		}
		
		// 获取 NodeInfo
		inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, inodeID)
		inodeItem, err := txn.Get([]byte(inodeKey))
		if err != nil {
			return err
		}
		
		var nodeInfo pb.NodeInfo
		err = inodeItem.Value(func(val []byte) error {
			return proto.Unmarshal(val, &nodeInfo)
		})
		if err != nil {
			return err
		}
		
		// 如果是目录，检查是否为空或需要递归删除
		if nodeInfo.Type == pb.FileType_Directory {
			children, err := fsm.listDirectoryInTx(txn, inodeID)
			if err != nil {
				return err
			}
			
			if len(children) > 0 && !op.Recursive {
				return fmt.Errorf("directory not empty: %s", path)
			}
			
			// 递归删除子节点
			if op.Recursive {
				for _, child := range children {
					if err := fsm.deleteNodeInTx(txn, child.Inode, child.Path); err != nil {
						return err
					}
				}
			}
		}
		
		// 删除节点本身
		return fsm.deleteNodeInTx(txn, inodeID, path)
	})
}

// applyFinalizeWrite 应用完成写入操作
func (fsm *MetadataFSM) applyFinalizeWrite(data json.RawMessage) error {
	var op FinalizeWriteOperation
	if err := json.Unmarshal(data, &op); err != nil {
		return err
	}
	
	log.Printf("[FSM] Finalizing write: inode=%d, size=%d, md5=%s", op.Inode, op.Size, op.MD5)
	
	return fsm.db.Update(func(txn *badger.Txn) error {
		// 更新 inode 元数据
		inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, op.Inode)
		item, err := txn.Get([]byte(inodeKey))
		if err != nil {
			return err
		}
		
		var nodeInfo pb.NodeInfo
		err = item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &nodeInfo)
		})
		if err != nil {
			return err
		}
		
		nodeInfo.Size = int64(op.Size)
		nodeInfo.Md5 = op.MD5
		nodeInfo.Mtime = time.Now().UnixMilli()
		
		inodeData, err := proto.Marshal(&nodeInfo)
		if err != nil {
			return err
		}
		
		if err := txn.Set([]byte(inodeKey), inodeData); err != nil {
			return err
		}
		
		// 保存块映射
		for i, block := range op.Blocks {
			blockLocs := &pb.BlockLocations{
				BlockId:   block.BlockID,
				Locations: block.DataNodes,
			}
			
			blockData, err := proto.Marshal(blockLocs)
			if err != nil {
				return err
			}
			
			blockKey := fmt.Sprintf("%s%d/%d", model.PrefixBlock, op.Inode, i)
			if err := txn.Set([]byte(blockKey), blockData); err != nil {
				return err
			}
		}
		
		log.Printf("[FSM] Write finalized: inode=%d, %d blocks saved", op.Inode, len(op.Blocks))
		return nil
	})
}

// applySetBlockMapping 应用设置块映射操作
func (fsm *MetadataFSM) applySetBlockMapping(data json.RawMessage) error {
	var op SetBlockMappingOperation
	if err := json.Unmarshal(data, &op); err != nil {
		return err
	}
	
	log.Printf("[FSM] Setting block mapping: inode=%d, index=%d, blockID=%d", 
		op.InodeID, op.BlockIndex, op.BlockID)
	
	return fsm.db.Update(func(txn *badger.Txn) error {
		blockLocs := &pb.BlockLocations{
			BlockId:   op.BlockID,
			Locations: op.Locations,
		}
		
		blockData, err := proto.Marshal(blockLocs)
		if err != nil {
			return err
		}
		
		key := fmt.Sprintf("%s%d/%d", model.PrefixBlock, op.InodeID, op.BlockIndex)
		return txn.Set([]byte(key), blockData)
	})
}

// applyUpdateBlockLocation 应用更新块位置操作
func (fsm *MetadataFSM) applyUpdateBlockLocation(data json.RawMessage) error {
	var op UpdateBlockLocationOperation
	if err := json.Unmarshal(data, &op); err != nil {
		return err
	}
	
	log.Printf("[FSM] Updating block location: blockID=%d, %s -> %s", 
		op.BlockID, op.OldAddr, op.NewAddr)
	
	return fsm.db.Update(func(txn *badger.Txn) error {
		// 遍历所有 inode 查找包含该块的文件
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		
		prefix := []byte(model.PrefixBlock)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			
			var blockLocs pb.BlockLocations
			err := item.Value(func(val []byte) error {
				return proto.Unmarshal(val, &blockLocs)
			})
			if err != nil {
				continue
			}
			
			if blockLocs.BlockId != op.BlockID {
				continue
			}
			
			// 更新位置
			updated := false
			for i, loc := range blockLocs.Locations {
				if loc == op.OldAddr {
					blockLocs.Locations[i] = op.NewAddr
					updated = true
					break
				}
			}
			
			if updated {
				blockData, err := proto.Marshal(&blockLocs)
				if err != nil {
					return err
				}
				return txn.Set(key, blockData)
			}
		}
		
		return nil
	})
}

// Snapshot 创建快照
func (fsm *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	log.Printf("[FSM] Creating snapshot")
	return &MetadataSnapshot{db: fsm.db}, nil
}

// Restore 从快照恢复
func (fsm *MetadataFSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	
	log.Printf("[FSM] Restoring from snapshot")
	
	// 使用 BadgerDB 的 Load 功能恢复数据
	return fsm.db.Load(snapshot, 256)
}

// 辅助方法

func (fsm *MetadataFSM) listDirectoryInTx(txn *badger.Txn, inodeID uint64) ([]*pb.NodeInfo, error) {
	var nodes []*pb.NodeInfo
	
	prefix := fmt.Sprintf("%s%d/", model.PrefixDir, inodeID)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	
	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()
		
		var childInodeID uint64
		err := item.Value(func(val []byte) error {
			childInodeID = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			continue
		}
		
		childInodeKey := fmt.Sprintf("%s%d", model.PrefixInode, childInodeID)
		childItem, err := txn.Get([]byte(childInodeKey))
		if err != nil {
			continue
		}
		
		var childNodeInfo pb.NodeInfo
		err = childItem.Value(func(val []byte) error {
			return proto.Unmarshal(val, &childNodeInfo)
		})
		if err != nil {
			continue
		}
		
		nodes = append(nodes, &childNodeInfo)
	}
	
	return nodes, nil
}

func (fsm *MetadataFSM) deleteNodeInTx(txn *badger.Txn, inodeID uint64, path string) error {
	// 删除 Inode 信息
	inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, inodeID)
	if err := txn.Delete([]byte(inodeKey)); err != nil {
		return err
	}
	
	// 删除路径映射
	pathKey := model.PrefixPath + path
	if err := txn.Delete([]byte(pathKey)); err != nil {
		return err
	}
	
	// 删除父目录中的条目
	if path != "/" {
		parentPath := filepath.Dir(path)
		fileName := filepath.Base(path)
		
		parentKey := model.PrefixPath + parentPath
		item, err := txn.Get([]byte(parentKey))
		if err == nil {
			var parentInodeID uint64
			item.Value(func(val []byte) error {
				parentInodeID = binary.BigEndian.Uint64(val)
				return nil
			})
			
			dirKey := fmt.Sprintf("%s%d/%s", model.PrefixDir, parentInodeID, fileName)
			txn.Delete([]byte(dirKey))
		}
	}
	
	// 删除块映射
	blockPrefix := fmt.Sprintf("%s%d/", model.PrefixBlock, inodeID)
	return fsm.deleteKeysWithPrefixInTx(txn, blockPrefix)
}

func (fsm *MetadataFSM) deleteKeysWithPrefixInTx(txn *badger.Txn, prefix string) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	
	var keysToDelete [][]byte
	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()
		key := make([]byte, len(item.Key()))
		copy(key, item.Key())
		keysToDelete = append(keysToDelete, key)
	}
	
	for _, key := range keysToDelete {
		if err := txn.Delete(key); err != nil {
			return err
		}
	}
	
	return nil
}

func (fsm *MetadataFSM) updateInodeCounterInTx(txn *badger.Txn, inodeID uint64) error {
	var currentCounter uint64
	item, err := txn.Get([]byte(model.InodeCounterKey))
	if err == badger.ErrKeyNotFound {
		currentCounter = 0
	} else if err != nil {
		return err
	} else {
		err = item.Value(func(val []byte) error {
			currentCounter = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return err
		}
	}
	
	if inodeID > currentCounter {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, inodeID)
		return txn.Set([]byte(model.InodeCounterKey), buf)
	}
	
	return nil
}

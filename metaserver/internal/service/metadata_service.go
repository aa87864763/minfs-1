package service

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"metaserver/internal/model"
	"metaserver/pb"

	"github.com/dgraph-io/badger/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MetadataService struct {
	db     *badger.DB
	config *model.Config
}

func NewMetadataService(db *badger.DB, config *model.Config) *MetadataService {
	return &MetadataService{
		db:     db,
		config: config,
	}
}

// generateInodeID 生成新的 Inode ID
func (ms *MetadataService) generateInodeID() (uint64, error) {
	var inodeID uint64
	
	err := ms.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(model.InodeCounterKey))
		if err == badger.ErrKeyNotFound {
			inodeID = 1
		} else if err != nil {
			return err
		} else {
			err = item.Value(func(val []byte) error {
				inodeID = binary.BigEndian.Uint64(val) + 1
				return nil
			})
			if err != nil {
				return err
			}
		}
		
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, inodeID)
		return txn.Set([]byte(model.InodeCounterKey), buf)
	})
	
	return inodeID, err
}

// CreateNode 创建文件或目录节点
func (ms *MetadataService) CreateNode(path string, nodeType pb.NodeType) error {
	// 标准化路径
	path = filepath.Clean(path)
	if path == "." {
		path = "/"
	}
	
	return ms.db.Update(func(txn *badger.Txn) error {
		// 检查路径是否已存在
		pathKey := model.PrefixPath + path
		_, err := txn.Get([]byte(pathKey))
		if err == nil {
			return fmt.Errorf("path already exists: %s", path)
		}
		if err != badger.ErrKeyNotFound {
			return err
		}
		
		// 检查父目录是否存在
		parentPath := filepath.Dir(path)
		if parentPath != "/" && parentPath != path {
			parentKey := model.PrefixPath + parentPath
			_, err := txn.Get([]byte(parentKey))
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("parent directory does not exist: %s", parentPath)
			}
			if err != nil {
				return err
			}
		}
		
		// 生成新的 Inode ID
		inodeID, err := ms.generateInodeIDInTx(txn)
		if err != nil {
			return err
		}
		
		// 创建 NodeInfo
		nodeInfo := &pb.NodeInfo{
			Inode:       inodeID,
			Path:        path,
			Type:        nodeType,
			Size:        0,
			ModTime:     timestamppb.Now(),
			Replication: uint32(ms.config.Cluster.DefaultReplication),
		}
		
		// 序列化并存储 Inode 信息
		data, err := proto.Marshal(nodeInfo)
		if err != nil {
			return err
		}
		
		inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, inodeID)
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
			parentInodeID, err := ms.getInodeIDByPathInTx(txn, parentPath)
			if err != nil {
				return err
			}
			
			// 添加目录条目
			dirKey := fmt.Sprintf("%s%d/%s", model.PrefixDir, parentInodeID, fileName)
			if err := txn.Set([]byte(dirKey), inodeBuf); err != nil {
				return err
			}
		}
		
		return nil
	})
}

// generateInodeIDInTx 在事务中生成 Inode ID
func (ms *MetadataService) generateInodeIDInTx(txn *badger.Txn) (uint64, error) {
	var inodeID uint64
	
	item, err := txn.Get([]byte(model.InodeCounterKey))
	if err == badger.ErrKeyNotFound {
		inodeID = 1
	} else if err != nil {
		return 0, err
	} else {
		err = item.Value(func(val []byte) error {
			inodeID = binary.BigEndian.Uint64(val) + 1
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, inodeID)
	err = txn.Set([]byte(model.InodeCounterKey), buf)
	
	return inodeID, err
}

// getInodeIDByPathInTx 在事务中通过路径获取 Inode ID
func (ms *MetadataService) getInodeIDByPathInTx(txn *badger.Txn, path string) (uint64, error) {
	pathKey := model.PrefixPath + path
	item, err := txn.Get([]byte(pathKey))
	if err != nil {
		return 0, err
	}
	
	var inodeID uint64
	err = item.Value(func(val []byte) error {
		inodeID = binary.BigEndian.Uint64(val)
		return nil
	})
	
	return inodeID, err
}

// GetNodeInfo 获取节点信息
func (ms *MetadataService) GetNodeInfo(path string) (*pb.NodeInfo, error) {
	path = filepath.Clean(path)
	if path == "." {
		path = "/"
	}
	
	var nodeInfo *pb.NodeInfo
	
	err := ms.db.View(func(txn *badger.Txn) error {
		// 获取 Inode ID
		inodeID, err := ms.getInodeIDByPathInTx(txn, path)
		if err != nil {
			return err
		}
		
		// 获取 NodeInfo
		inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, inodeID)
		item, err := txn.Get([]byte(inodeKey))
		if err != nil {
			return err
		}
		
		return item.Value(func(val []byte) error {
			nodeInfo = &pb.NodeInfo{}
			return proto.Unmarshal(val, nodeInfo)
		})
	})
	
	return nodeInfo, err
}

// ListDirectory 列出目录内容
func (ms *MetadataService) ListDirectory(path string) ([]*pb.NodeInfo, error) {
	path = filepath.Clean(path)
	if path == "." {
		path = "/"
	}
	
	var nodes []*pb.NodeInfo
	
	err := ms.db.View(func(txn *badger.Txn) error {
		// 获取目录的 Inode ID
		inodeID, err := ms.getInodeIDByPathInTx(txn, path)
		if err != nil {
			return err
		}
		
		// 遍历目录条目
		prefix := fmt.Sprintf("%s%d/", model.PrefixDir, inodeID)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			
			// 获取子节点的 Inode ID
			var childInodeID uint64
			err := item.Value(func(val []byte) error {
				childInodeID = binary.BigEndian.Uint64(val)
				return nil
			})
			if err != nil {
				continue
			}
			
			// 获取子节点的 NodeInfo
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
		
		return nil
	})
	
	return nodes, err
}

// DeleteNode 删除节点
func (ms *MetadataService) DeleteNode(path string, recursive bool) ([]uint64, error) {
	path = filepath.Clean(path)
	if path == "." {
		path = "/"
	}
	
	if path == "/" {
		return nil, fmt.Errorf("cannot delete root directory")
	}
	
	var blocksToDelete []uint64
	
	err := ms.db.Update(func(txn *badger.Txn) error {
		// 获取节点信息
		inodeID, err := ms.getInodeIDByPathInTx(txn, path)
		if err != nil {
			return err
		}
		
		inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, inodeID)
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
		
		// 如果是目录，检查是否为空或需要递归删除
		if nodeInfo.Type == pb.NodeType_DIRECTORY {
			children, err := ms.listDirectoryInTx(txn, inodeID)
			if err != nil {
				return err
			}
			
			if len(children) > 0 && !recursive {
				return fmt.Errorf("directory not empty: %s", path)
			}
			
			// 递归删除子节点
			if recursive {
				for _, child := range children {
					childBlocks, err := ms.deleteNodeRecursiveInTx(txn, child.Inode, child.Path)
					if err != nil {
						return err
					}
					blocksToDelete = append(blocksToDelete, childBlocks...)
				}
			}
		} else {
			// 如果是文件，收集需要删除的块
			blocks, err := ms.getFileBlocksInTx(txn, inodeID)
			if err != nil {
				return err
			}
			blocksToDelete = append(blocksToDelete, blocks...)
		}
		
		// 删除节点本身
		return ms.deleteNodeInTx(txn, inodeID, path)
	})
	
	return blocksToDelete, err
}

// deleteNodeRecursiveInTx 在事务中递归删除节点
func (ms *MetadataService) deleteNodeRecursiveInTx(txn *badger.Txn, inodeID uint64, path string) ([]uint64, error) {
	var blocksToDelete []uint64
	
	// 获取节点信息
	inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, inodeID)
	item, err := txn.Get([]byte(inodeKey))
	if err != nil {
		return nil, err
	}
	
	var nodeInfo pb.NodeInfo
	err = item.Value(func(val []byte) error {
		return proto.Unmarshal(val, &nodeInfo)
	})
	if err != nil {
		return nil, err
	}
	
	// 如果是目录，递归删除子节点
	if nodeInfo.Type == pb.NodeType_DIRECTORY {
		children, err := ms.listDirectoryInTx(txn, inodeID)
		if err != nil {
			return nil, err
		}
		
		for _, child := range children {
			childBlocks, err := ms.deleteNodeRecursiveInTx(txn, child.Inode, child.Path)
			if err != nil {
				return nil, err
			}
			blocksToDelete = append(blocksToDelete, childBlocks...)
		}
	} else {
		// 如果是文件，收集需要删除的块
		blocks, err := ms.getFileBlocksInTx(txn, inodeID)
		if err != nil {
			return nil, err
		}
		blocksToDelete = append(blocksToDelete, blocks...)
	}
	
	// 删除节点本身
	err = ms.deleteNodeInTx(txn, inodeID, path)
	return blocksToDelete, err
}

// deleteNodeInTx 在事务中删除单个节点
func (ms *MetadataService) deleteNodeInTx(txn *badger.Txn, inodeID uint64, path string) error {
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
		
		parentInodeID, err := ms.getInodeIDByPathInTx(txn, parentPath)
		if err != nil {
			return err
		}
		
		dirKey := fmt.Sprintf("%s%d/%s", model.PrefixDir, parentInodeID, fileName)
		if err := txn.Delete([]byte(dirKey)); err != nil {
			return err
		}
	}
	
	// 删除所有相关的块映射
	blockPrefix := fmt.Sprintf("%s%d/", model.PrefixBlock, inodeID)
	return ms.deleteKeysWithPrefixInTx(txn, blockPrefix)
}

// listDirectoryInTx 在事务中列出目录内容
func (ms *MetadataService) listDirectoryInTx(txn *badger.Txn, inodeID uint64) ([]*pb.NodeInfo, error) {
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

// getFileBlocksInTx 在事务中获取文件的所有块ID
func (ms *MetadataService) getFileBlocksInTx(txn *badger.Txn, inodeID uint64) ([]uint64, error) {
	var blocks []uint64
	
	prefix := fmt.Sprintf("%s%d/", model.PrefixBlock, inodeID)
	opts := badger.DefaultIteratorOptions
	it := txn.NewIterator(opts)
	defer it.Close()
	
	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()
		
		err := item.Value(func(val []byte) error {
			var blockLocs pb.BlockLocations
			if err := proto.Unmarshal(val, &blockLocs); err != nil {
				return err
			}
			blocks = append(blocks, blockLocs.BlockId)
			return nil
		})
		if err != nil {
			continue
		}
	}
	
	return blocks, nil
}

// deleteKeysWithPrefixInTx 在事务中删除具有指定前缀的所有键
func (ms *MetadataService) deleteKeysWithPrefixInTx(txn *badger.Txn, prefix string) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	
	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()
		if err := txn.Delete(item.Key()); err != nil {
			return err
		}
	}
	
	return nil
}

// SetBlockMapping 设置文件块映射
func (ms *MetadataService) SetBlockMapping(inodeID uint64, blockIndex uint64, blockLocs *pb.BlockLocations) error {
	return ms.db.Update(func(txn *badger.Txn) error {
		data, err := proto.Marshal(blockLocs)
		if err != nil {
			return err
		}
		
		key := fmt.Sprintf("%s%d/%d", model.PrefixBlock, inodeID, blockIndex)
		return txn.Set([]byte(key), data)
	})
}

// GetBlockMappings 获取文件的所有块映射
func (ms *MetadataService) GetBlockMappings(inodeID uint64) ([]*pb.BlockLocations, error) {
	var blockMappings []*pb.BlockLocations
	
	err := ms.db.View(func(txn *badger.Txn) error {
		prefix := fmt.Sprintf("%s%d/", model.PrefixBlock, inodeID)
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			
			err := item.Value(func(val []byte) error {
				var blockLocs pb.BlockLocations
				if err := proto.Unmarshal(val, &blockLocs); err != nil {
					return err
				}
				blockMappings = append(blockMappings, &blockLocs)
				return nil
			})
			if err != nil {
				return err
			}
		}
		
		return nil
	})
	
	return blockMappings, err
}

// FinalizeWrite 完成文件写入，更新文件大小和修改时间
func (ms *MetadataService) FinalizeWrite(path string, inodeID uint64, size uint64) error {
	path = filepath.Clean(path)
	if path == "." {
		path = "/"
	}
	
	return ms.db.Update(func(txn *badger.Txn) error {
		// 获取当前的 NodeInfo
		inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, inodeID)
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
		
		// 更新文件信息
		nodeInfo.Size = size
		nodeInfo.ModTime = timestamppb.Now()
		
		// 重新序列化并保存
		data, err := proto.Marshal(&nodeInfo)
		if err != nil {
			return err
		}
		
		return txn.Set([]byte(inodeKey), data)
	})
}

// AddGCEntry 添加垃圾回收条目
func (ms *MetadataService) AddGCEntry(blockID uint64, locations []string) error {
	return ms.db.Update(func(txn *badger.Txn) error {
		gcEntry := model.GCEntry{
			BlockID:    blockID,
			DeleteTime: time.Now(),
			Locations:  locations,
			Status:     "pending",
			SentTime:   time.Time{}, // 初始化为零值
		}
		
		data, err := json.Marshal(gcEntry)
		if err != nil {
			return err
		}
		
		key := fmt.Sprintf("%s%d", model.PrefixGC, blockID)
		return txn.Set([]byte(key), data)
	})
}

// GetGCEntries 获取所有垃圾回收条目
func (ms *MetadataService) GetGCEntries() ([]model.GCEntry, error) {
	var entries []model.GCEntry
	
	err := ms.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		
		prefix := []byte(model.PrefixGC)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			
			err := item.Value(func(val []byte) error {
				var entry model.GCEntry
				if err := json.Unmarshal(val, &entry); err != nil {
					return err
				}
				entries = append(entries, entry)
				return nil
			})
			if err != nil {
				continue
			}
		}
		
		return nil
	})
	
	return entries, err
}

// UpdateGCEntryStatus 更新GC条目状态
func (ms *MetadataService) UpdateGCEntryStatus(blockID uint64, status string) error {
	return ms.db.Update(func(txn *badger.Txn) error {
		key := fmt.Sprintf("%s%d", model.PrefixGC, blockID)
		
		// 先读取现有条目
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		
		var entry model.GCEntry
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &entry)
		})
		if err != nil {
			return err
		}
		
		// 更新状态
		entry.Status = status
		if status == "sent" {
			entry.SentTime = time.Now()
		}
		
		// 保存更新后的条目
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		
		return txn.Set([]byte(key), data)
	})
}

// RemoveGCEntry 移除垃圾回收条目
func (ms *MetadataService) RemoveGCEntry(blockID uint64) error {
	return ms.db.Update(func(txn *badger.Txn) error {
		key := fmt.Sprintf("%s%d", model.PrefixGC, blockID)
		return txn.Delete([]byte(key))
	})
}
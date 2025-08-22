package service

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"metaServer/internal/model"
	"metaServer/pb"

	"github.com/dgraph-io/badger/v3"
	"google.golang.org/protobuf/proto"
)

type MetadataService struct {
	db         *badger.DB
	config     *model.Config
	walService *WALService
}

func NewMetadataService(db *badger.DB, config *model.Config, walService *WALService) *MetadataService {
	return &MetadataService{
		db:         db,
		config:     config,
		walService: walService,
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
func (ms *MetadataService) CreateNode(path string, nodeType pb.FileType) error {
	return ms.CreateNodeWithInode(path, nodeType, nil)
}

// CreateNodeWithInode 创建文件或目录节点，可以指定Inode ID（用于WAL回放）
func (ms *MetadataService) CreateNodeWithInode(path string, nodeType pb.FileType, inodeID *uint64) error {
	// 转换 FileType 为内部使用的 NodeType
	var internalType pb.FileType
	switch nodeType {
	case pb.FileType_Directory:
		internalType = pb.FileType_Directory
	case pb.FileType_File:
		internalType = pb.FileType_File
	default:
		internalType = pb.FileType_File
	}
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

		// 生成新的 Inode ID 或使用指定的 Inode ID
		var nodeInodeID uint64
		if inodeID != nil {
			// 使用指定的 Inode ID（WAL回放模式）
			nodeInodeID = *inodeID

			// 验证指定的 Inode ID 是否已被使用
			inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, nodeInodeID)
			_, err := txn.Get([]byte(inodeKey))
			if err == nil {
				return fmt.Errorf("inode ID %d already exists", nodeInodeID)
			}
			if err != badger.ErrKeyNotFound {
				return err
			}

			// 如果指定的 Inode ID 比当前计数器大，需要更新计数器
			err = ms.updateInodeCounterIfNeededInTx(txn, nodeInodeID)
			if err != nil {
				return err
			}
		} else {
			// 正常模式：生成新的 Inode ID
			var generateErr error
			nodeInodeID, generateErr = ms.generateInodeIDInTx(txn)
			if generateErr != nil {
				return generateErr
			}
		}

		// 创建 NodeInfo
		nodeInfo := &pb.NodeInfo{
			Inode:       nodeInodeID,
			Path:        path,
			Type:        internalType,
			Size:        0,
			Mtime:       time.Now().UnixMilli(),
			Replication: uint32(ms.config.Cluster.DefaultReplication),
		}

		// 序列化并存储 Inode 信息
		data, err := proto.Marshal(nodeInfo)
		if err != nil {
			return err
		}

		inodeKey := fmt.Sprintf("%s%d", model.PrefixInode, nodeInodeID)
		if err := txn.Set([]byte(inodeKey), data); err != nil {
			return err
		}

		// 存储路径映射
		inodeBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(inodeBuf, nodeInodeID)
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

		err = item.Value(func(val []byte) error {
			nodeInfo = &pb.NodeInfo{}
			return proto.Unmarshal(val, nodeInfo)
		})
		if err != nil {
			return err
		}

		// 如果是目录，计算其总大小（递归计算子文件和子目录大小）
		if nodeInfo.Type == pb.FileType_Directory {
			totalSize, err := ms.calculateDirectorySizeInTx(txn, inodeID)
			if err != nil {
				// 如果计算失败，记录错误但不中断操作，使用原始大小
				fmt.Printf("Warning: failed to calculate directory size for %s: %v\n", path, err)
			} else {
				nodeInfo.Size = totalSize
			}
		}

		return nil
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

			// 如果是目录，计算其总大小
			if childNodeInfo.Type == pb.FileType_Directory {
				totalSize, err := ms.calculateDirectorySizeInTx(txn, childInodeID)
				if err != nil {
					// 如果计算失败，记录错误但不中断操作，使用原始大小
					fmt.Printf("Warning: failed to calculate directory size for inode %d: %v\n", childInodeID, err)
				} else {
					childNodeInfo.Size = totalSize
				}
			}

			nodes = append(nodes, &childNodeInfo)
		}

		return nil
	})

	return nodes, err
}

// DeleteNode 删除节点
func (ms *MetadataService) DeleteNode(path string, recursive bool) ([]model.BlockWithLocations, error) {
	path = filepath.Clean(path)
	if path == "." {
		path = "/"
	}

	if path == "/" {
		return nil, fmt.Errorf("cannot delete root directory")
	}

	var blocksToDelete []model.BlockWithLocations

	err := ms.db.Update(func(txn *badger.Txn) error {
		// 关键修复：每次事务重试时清空blocksToDelete，避免重复累积
		blocksToDelete = blocksToDelete[:0]
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
		if nodeInfo.Type == pb.FileType_Directory {
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
					childBlocks, err := ms.deleteNodeRecursiveWithLocationsInTx(txn, child.Inode, child.Path)
					if err != nil {
						return err
					}
					blocksToDelete = append(blocksToDelete, childBlocks...)
				}
			}
		} else {
			// 如果是文件，收集需要删除的块及其位置信息
			blocks, err := ms.getFileBlocksWithLocationsInTx(txn, inodeID)
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

// deleteNodeRecursiveWithLocationsInTx 在事务中递归删除节点（返回带位置信息的块）
func (ms *MetadataService) deleteNodeRecursiveWithLocationsInTx(txn *badger.Txn, inodeID uint64, path string) ([]model.BlockWithLocations, error) {
	var blocksToDelete []model.BlockWithLocations

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
	if nodeInfo.Type == pb.FileType_Directory {
		children, err := ms.listDirectoryInTx(txn, inodeID)
		if err != nil {
			return nil, err
		}

		for _, child := range children {
			childBlocks, err := ms.deleteNodeRecursiveWithLocationsInTx(txn, child.Inode, child.Path)
			if err != nil {
				return nil, err
			}
			blocksToDelete = append(blocksToDelete, childBlocks...)
		}
	} else {
		// 如果是文件，收集需要删除的块及其位置信息
		blocks, err := ms.getFileBlocksWithLocationsInTx(txn, inodeID)
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

// getFileBlocksWithLocationsInTx 在事务中获取文件的所有块及其位置信息
func (ms *MetadataService) getFileBlocksWithLocationsInTx(txn *badger.Txn, inodeID uint64) ([]model.BlockWithLocations, error) {
	var blocks []model.BlockWithLocations

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
			block := model.BlockWithLocations{
				BlockID:   blockLocs.BlockId,
				Locations: blockLocs.Locations,
			}
			blocks = append(blocks, block)
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

	// 关键修复：先收集所有要删除的键，避免在迭代中删除导致迭代器状态混乱
	var keysToDelete [][]byte
	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()
		key := make([]byte, len(item.Key()))
		copy(key, item.Key()) // 复制键，避免引用问题
		keysToDelete = append(keysToDelete, key)
	}

	// 迭代器完成后，安全地删除所有收集到的键
	deletedCount := 0
	for _, key := range keysToDelete {
		if err := txn.Delete(key); err != nil {
			return err
		}
		deletedCount++
	}

	return nil
}

// SetBlockMapping 设置文件块映射（带WAL日志）
func (ms *MetadataService) SetBlockMapping(inodeID uint64, blockIndex uint64, blockLocs *pb.BlockLocations) error {
	// 1. 先写WAL日志
	if ms.walService != nil {
		// 构建操作数据
		operation := &pb.SetBlockMappingOperation{
			InodeId:    inodeID,
			BlockIndex: blockIndex,
			BlockLocs:  blockLocs,
		}

		// 写入日志
		entry, err := ms.walService.AppendLogEntry(pb.WALOperationType_SET_BLOCK_MAPPING, operation)
		if err != nil {
			return fmt.Errorf("failed to write WAL for SetBlockMapping: %v", err)
		}

		// 如果是Leader，需要将此日志同步给Followers
		if entry != nil && ms.walService.IsLeader() {
			// 异步同步，不阻塞主流程
			go ms.walService.SyncToFollowers(entry)
		}
	}

	// 2. 再执行数据库更新
	return ms.setBlockMappingInDB(inodeID, blockIndex, blockLocs)
}

// setBlockMappingInDB 设置文件块映射（仅数据库操作，不写WAL）
func (ms *MetadataService) setBlockMappingInDB(inodeID uint64, blockIndex uint64, blockLocs *pb.BlockLocations) error {
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
	// 使用map临时存储，key为块索引，value为BlockLocations
	blockMap := make(map[uint64]*pb.BlockLocations)

	err := ms.db.View(func(txn *badger.Txn) error {
		prefix := fmt.Sprintf("%s%d/", model.PrefixBlock, inodeID)
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()

			// 从key中提取块索引
			key := string(item.Key())
			// key格式: "b/{inodeID}/{blockIndex}"
			// 移除前缀，获取块索引部分
			if !strings.HasPrefix(key, prefix) {
				continue
			}

			blockIndexStr := key[len(prefix):]
			blockIndex, err := strconv.ParseUint(blockIndexStr, 10, 64)
			if err != nil {
				// 如果解析失败，跳过这个key
				continue
			}

			err = item.Value(func(val []byte) error {
				var blockLocs pb.BlockLocations
				if err := proto.Unmarshal(val, &blockLocs); err != nil {
					return err
				}
				blockMap[blockIndex] = &blockLocs
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

	// 按块索引排序
	var indices []uint64
	for index := range blockMap {
		indices = append(indices, index)
	}

	// 对索引进行排序
	sort.Slice(indices, func(i, j int) bool {
		return indices[i] < indices[j]
	})

	// 按排序后的顺序构建结果
	var blockMappings []*pb.BlockLocations
	for _, index := range indices {
		blockMappings = append(blockMappings, blockMap[index])
	}

	return blockMappings, nil
}

// FinalizeWrite 完成文件写入，更新文件大小、修改时间和MD5哈希
func (ms *MetadataService) FinalizeWrite(path string, inodeID uint64, size uint64, md5Hash string) error {
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
		nodeInfo.Size = int64(size)
		nodeInfo.Mtime = time.Now().UnixMilli()
		nodeInfo.Md5 = md5Hash

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

// TraverseAllFiles 遍历所有文件节点
func (ms *MetadataService) TraverseAllFiles(callback func(*pb.NodeInfo) error) error {
	return ms.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		// 遍历所有 inode 条目
		inodePrefix := []byte(model.PrefixInode)
		for it.Seek(inodePrefix); it.ValidForPrefix(inodePrefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var nodeInfo pb.NodeInfo
				if err := proto.Unmarshal(val, &nodeInfo); err != nil {
					return err
				}

				return callback(&nodeInfo)
			})

			if err != nil {
				return err
			}
		}

		return nil
	})
}

// UpdateBlockLocation 更新块位置信息，将旧地址替换为新地址
func (ms *MetadataService) UpdateBlockLocation(blockID uint64, oldAddr, newAddr string) error {
	// 1. 先写WAL日志（如果WAL服务可用）
	if ms.walService != nil {
		operation := &pb.UpdateBlockLocationOperation{
			BlockId: blockID,
			OldAddr: oldAddr,
			NewAddr: newAddr,
		}
		entry, err := ms.walService.AppendLogEntry(pb.WALOperationType_UPDATE_BLOCK_LOCATION, operation)
		if err != nil {
			return fmt.Errorf("failed to write WAL entry for block location update: %v", err)
		}

		// 如果是Leader，需要将此日志异步同步给Followers
		if entry != nil && ms.walService.IsLeader() {
			// 异步同步，不阻塞主流程
			go ms.walService.SyncToFollowers(entry)
		}
	}

	// 2. 执行实际的数据库更新
	return ms.updateBlockLocationInDB(blockID, oldAddr, newAddr)
}

// updateBlockLocationInDB 在数据库中更新块位置信息（不写WAL）
func (ms *MetadataService) updateBlockLocationInDB(blockID uint64, oldAddr, newAddr string) error {
	return ms.db.Update(func(txn *badger.Txn) error {
		// 需要遍历所有包含该块的文件，找到对应的块映射并更新
		inodePrefix := []byte(model.PrefixInode)
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(inodePrefix); it.ValidForPrefix(inodePrefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var nodeInfo pb.NodeInfo
				if err := proto.Unmarshal(val, &nodeInfo); err != nil {
					return err
				}

				// 只处理文件类型
				if nodeInfo.Type != pb.FileType_File {
					return nil
				}

				// 检查这个文件的块映射
				return ms.updateFileBlockLocation(txn, nodeInfo.Inode, blockID, oldAddr, newAddr)
			})

			if err != nil {
				return err
			}
		}

		return nil
	})
}

// updateFileBlockLocation 更新单个文件的块位置信息
func (ms *MetadataService) updateFileBlockLocation(txn *badger.Txn, inodeID, blockID uint64, oldAddr, newAddr string) error {
	// 遍历该文件的所有块映射
	prefix := fmt.Sprintf("%s%d/", model.PrefixBlock, inodeID)
	opts := badger.DefaultIteratorOptions
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()
		key := item.Key()

		var blockLocs pb.BlockLocations
		err := item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &blockLocs)
		})
		if err != nil {
			continue
		}

		// 检查这个块是否是我们要更新的
		if blockLocs.BlockId != blockID {
			continue
		}

		// 更新位置信息
		updated := false
		for i, location := range blockLocs.Locations {
			if location == oldAddr {
				blockLocs.Locations[i] = newAddr
				updated = true
				break
			}
		}

		// 如果有更新，保存回数据库
		if updated {
			data, err := proto.Marshal(&blockLocs)
			if err != nil {
				return fmt.Errorf("failed to marshal updated block locations: %v", err)
			}

			err = txn.Set(key, data)
			if err != nil {
				return fmt.Errorf("failed to update block location in database: %v", err)
			}

			return nil // 找到并更新了，可以返回
		}
	}

	return nil
}

// calculateDirectorySizeInTx 在事务中递归计算目录的总大小
func (ms *MetadataService) calculateDirectorySizeInTx(txn *badger.Txn, dirInodeID uint64) (int64, error) {
	var totalSize int64 = 0

	// 遍历目录中的所有子项
	prefix := fmt.Sprintf("%s%d/", model.PrefixDir, dirInodeID)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()

		// 获取子项的 Inode ID
		var childInodeID uint64
		err := item.Value(func(val []byte) error {
			childInodeID = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			continue
		}

		// 获取子项的 NodeInfo
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

		if childNodeInfo.Type == pb.FileType_File {
			// 如果是文件，直接累加文件大小
			totalSize += childNodeInfo.Size
		} else if childNodeInfo.Type == pb.FileType_Directory {
			// 如果是目录，递归计算子目录大小
			subDirSize, err := ms.calculateDirectorySizeInTx(txn, childInodeID)
			if err != nil {
				// 递归计算失败，记录警告但继续处理其他子项
				fmt.Printf("Warning: failed to calculate subdirectory size for inode %d: %v\n", childInodeID, err)
				continue
			}
			totalSize += subDirSize
		}
	}

	return totalSize, nil
}

// updateInodeCounterIfNeededInTx 如果指定的 Inode ID 比当前计数器大，则更新计数器
func (ms *MetadataService) updateInodeCounterIfNeededInTx(txn *badger.Txn, inodeID uint64) error {
	// 获取当前计数器值
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

	// 如果指定的 Inode ID 比当前计数器大，更新计数器
	if inodeID > currentCounter {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, inodeID)
		return txn.Set([]byte(model.InodeCounterKey), buf)
	}

	return nil
}

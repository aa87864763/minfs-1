package service

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"dataServer/internal/model"
)

// LocalStorageService 本地存储服务实现
type LocalStorageService struct {
	rootDir string
	mu      sync.RWMutex
}

// NewStorageService 创建新的存储服务实例
func NewStorageService(rootDir string) (*LocalStorageService, error) {
	// 确保根目录存在
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory %s: %w", rootDir, err)
	}

	return &LocalStorageService{
		rootDir: rootDir,
	}, nil
}

// WriteBlock 将数据块写入本地文件系统
// 使用哈希目录结构，例如: /data/f1/8b/f18be298c765.dat
func (s *LocalStorageService) WriteBlock(blockID uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := s.getBlockFilePath(blockID)

	// 确保目录存在
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// 创建临时文件，原子性写入
	tempFile := filePath + ".tmp"
	file, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file %s: %w", tempFile, err)
	}
	defer file.Close()

	// 写入数据
	if _, err := file.Write(data); err != nil {
		os.Remove(tempFile) // 清理临时文件
		return fmt.Errorf("failed to write data to file: %w", err)
	}

	// 确保数据写入磁盘
	if err := file.Sync(); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("failed to sync file: %w", err)
	}

	file.Close()

	// 原子性重命名
	if err := os.Rename(tempFile, filePath); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// ReadBlock 从本地文件系统读取数据块
func (s *LocalStorageService) ReadBlock(blockID uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := s.getBlockFilePath(blockID)

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("block %d not found", blockID)
		}
		return nil, fmt.Errorf("failed to read block %d: %w", blockID, err)
	}

	return data, nil
}

// DeleteBlock 删除本地数据块文件
func (s *LocalStorageService) DeleteBlock(blockID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := s.getBlockFilePath(blockID)

	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			return nil // 文件不存在，认为删除成功
		}
		return fmt.Errorf("failed to delete block %d: %w", blockID, err)
	}

	return nil
}

// BlockExists 检查数据块是否存在
func (s *LocalStorageService) BlockExists(blockID uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := s.getBlockFilePath(blockID)
	_, err := os.Stat(filePath)
	return err == nil
}

// ListBlocks 列出所有存储的数据块ID
func (s *LocalStorageService) ListBlocks() ([]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var blockIds []uint64

	// 遍历所有子目录
	err := filepath.Walk(s.rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 跳过目录
		if info.IsDir() {
			return nil
		}

		// 检查是否是数据块文件
		if strings.HasSuffix(path, ".dat") {
			if blockID := s.extractBlockIDFromPath(path); blockID != 0 {
				blockIds = append(blockIds, blockID)
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk directory: %w", err)
	}

	return blockIds, nil
}

// GetStat 获取存储统计信息
func (s *LocalStorageService) GetStat() (*model.StorageStat, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 获取所有块ID
	blockIds, err := s.ListBlocks()
	if err != nil {
		return nil, err
	}

	// 计算已使用空间
	var usedSpace uint64
	for _, blockID := range blockIds {
		filePath := s.getBlockFilePath(blockID)
		if info, err := os.Stat(filePath); err == nil {
			usedSpace += uint64(info.Size())
		}
	}

	// 获取可用空间和总容量
	freeSpace, totalCapacity, err := s.getDiskSpaceInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get disk space info: %w", err)
	}

	return &model.StorageStat{
		BlockCount:    uint64(len(blockIds)),
		FreeSpace:     freeSpace,
		UsedSpace:     usedSpace,
		TotalCapacity: totalCapacity,
		BlockIds:      blockIds,
	}, nil
}

// getBlockFilePath 根据块ID生成文件路径
// 直接使用BlockID作为文件名，便于查找和调试
func (s *LocalStorageService) getBlockFilePath(blockID uint64) string {
	fileName := fmt.Sprintf("%d.dat", blockID)
	return filepath.Join(s.rootDir, fileName)
}

// extractBlockIDFromPath 从文件路径提取块ID
func (s *LocalStorageService) extractBlockIDFromPath(path string) uint64 {
	fileName := filepath.Base(path)
	if !strings.HasSuffix(fileName, ".dat") {
		return 0
	}

	// 去掉 .dat 扩展名
	idStr := strings.TrimSuffix(fileName, ".dat")

	// 从十进制字符串解析为uint64
	var blockID uint64
	if _, err := fmt.Sscanf(idStr, "%d", &blockID); err != nil {
		return 0
	}

	return blockID
}

// getDiskSpaceInfo 获取磁盘空间信息（可用空间和总容量）
func (s *LocalStorageService) getDiskSpaceInfo() (freeSpace, totalCapacity uint64, err error) {
	var stat syscall.Statfs_t
	err = syscall.Statfs(s.rootDir, &stat)
	if err != nil {
		return 0, 0, err
	}

	// 可用空间 = 可用块数 * 块大小
	freeSpace = stat.Bavail * uint64(stat.Bsize)
	// 总容量 = 总块数 * 块大小
	totalCapacity = stat.Blocks * uint64(stat.Bsize)

	return freeSpace, totalCapacity, nil
}

// cleanupEmptyDirectories 递归清理空的父目录
// 只会清理到 rootDir，不会删除 rootDir 本身
func (s *LocalStorageService) cleanupEmptyDirectories(dirPath string) {
	// 不删除 rootDir 本身
	if dirPath == s.rootDir {
		return
	}

	// 确保路径在 rootDir 内
	if !strings.HasPrefix(dirPath, s.rootDir) {
		return
	}

	// 检查目录是否为空
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		// 目录不存在或无法读取，停止清理
		return
	}

	// 如果目录不为空，停止清理
	if len(entries) > 0 {
		return
	}

	// 目录为空，尝试删除
	if err := os.Remove(dirPath); err != nil {
		// 删除失败，停止清理（可能是权限问题等）
		fmt.Printf("Warning: Failed to remove empty directory %s: %v\n", dirPath, err)
		return
	}

	fmt.Printf("Cleaned up empty directory: %s\n", dirPath)

	// 如果删除成功，继续清理父目录
	parentDir := filepath.Dir(dirPath)
	if parentDir != dirPath { // 避免无限递归
		s.cleanupEmptyDirectories(parentDir)
	}
}

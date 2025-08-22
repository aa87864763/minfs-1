package model

import (
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 配置结构体
type Config struct {
	Server struct {
		Port     int `yaml:"port"`
		GrpcPort int `yaml:"grpc_port"`
	} `yaml:"server"`

	Database struct {
		BadgerDir string `yaml:"badger_dir"`
	} `yaml:"database"`

	Cluster struct {
		DefaultReplication     int           `yaml:"default_replication"`
		HeartbeatTimeout       time.Duration `yaml:"heartbeat_timeout"`
		PermanentDownThreshold time.Duration `yaml:"permanent_down_threshold"`
	} `yaml:"cluster"`

	Etcd struct {
		Endpoints           []string      `yaml:"endpoints"`
		Timeout             time.Duration `yaml:"timeout"`
		DataServerKeyPrefix string        `yaml:"dataServer_key_prefix"`
	} `yaml:"etcd"`

	Scheduler struct {
		FSCKInterval         time.Duration `yaml:"fsck_interval"`
		GCInterval           time.Duration `yaml:"gc_interval"`
		BlockSize            uint64        `yaml:"block_size"`
		FSCKWorkers          int           `yaml:"fsck_workers"`
		RepairWorkers        int           `yaml:"repair_workers"`
		RepairQueueSize      int           `yaml:"repair_queue_size"`
		MaxConcurrentRepairs int           `yaml:"max_concurrent_repairs"`
	} `yaml:"scheduler"`

	Logging struct {
		Level string `yaml:"level"`
		File  string `yaml:"file"`
	} `yaml:"logging"`
}

// LoadConfig 从文件加载配置
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// DataServerInfo 存储 DataServer 的运行时状态信息
type DataServerInfo struct {
	ID             string          // DataServer 唯一标识符
	Addr           string          // DataServer 地址 (IP:Port)
	BlockCount     uint64          // 当前存储的块数量
	FreeSpace      uint64          // 剩余存储空间 (字节)
	TotalCapacity  uint64          // 总存储容量 (字节)
	LastHeartbeat  time.Time       // 最后心跳时间
	IsHealthy      bool            // 是否健康 (基于心跳超时判断)
	ReportedBlocks map[uint64]bool // 当前报告的块列表

	// 用于调度算法的轮询计数器
	RoundRobinIndex int

	// 节点宕机相关
	UnhealthyStartTime *time.Time // 节点变为不健康的开始时间
	IsPermanentlyDown  bool       // 是否被标记为永久宕机

	// 线程安全锁
	mutex sync.RWMutex
}

// UpdateStatus 更新 DataServer 状态 (线程安全)
func (ds *DataServerInfo) UpdateStatus(blockCount, freeSpace, totalCapacity uint64) {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	ds.BlockCount = blockCount
	ds.FreeSpace = freeSpace
	ds.TotalCapacity = totalCapacity
	ds.LastHeartbeat = time.Now()
	ds.IsHealthy = true
}

// UpdateReportedBlocks 更新报告的块列表，返回新增的块ID列表
func (ds *DataServerInfo) UpdateReportedBlocks(blockIDs []uint64) []uint64 {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	// 记录新增的块
	var newBlocks []uint64
	newReportedBlocks := make(map[uint64]bool)

	for _, blockID := range blockIDs {
		newReportedBlocks[blockID] = true
		// 如果这是一个新块（之前没有报告过）
		if !ds.ReportedBlocks[blockID] {
			newBlocks = append(newBlocks, blockID)
		}
	}

	// 更新块列表
	ds.ReportedBlocks = newReportedBlocks

	return newBlocks
}

// HasBlock 检查是否包含指定的块
func (ds *DataServerInfo) HasBlock(blockID uint64) bool {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()

	return ds.ReportedBlocks[blockID]
}

// GetStatus 获取 DataServer 状态 (线程安全读取)
func (ds *DataServerInfo) GetStatus() (blockCount, freeSpace, totalCapacity uint64, lastHeartbeat time.Time, isHealthy bool) {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()

	return ds.BlockCount, ds.FreeSpace, ds.TotalCapacity, ds.LastHeartbeat, ds.IsHealthy
}

// MarkUnhealthy 标记 DataServer 为不健康状态
func (ds *DataServerInfo) MarkUnhealthy() {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	if ds.IsHealthy {
		// 第一次标记为不健康，记录开始时间
		now := time.Now()
		ds.UnhealthyStartTime = &now
	}
	ds.IsHealthy = false
}

// GetReportedBlocks 获取报告的块列表 (线程安全读取)
func (ds *DataServerInfo) GetReportedBlocks() map[uint64]bool {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()

	// 创建副本以避免外部修改
	blocks := make(map[uint64]bool)
	for blockID, exists := range ds.ReportedBlocks {
		blocks[blockID] = exists
	}
	return blocks
}

// MarkPermanentlyDown 标记节点为永久宕机
func (ds *DataServerInfo) MarkPermanentlyDown() {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()
	ds.IsPermanentlyDown = true
}

// IsPermanentlyDownStatus 检查节点是否被标记为永久宕机
func (ds *DataServerInfo) IsPermanentlyDownStatus() bool {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()
	return ds.IsPermanentlyDown
}

// GetUnhealthyDuration 获取节点不健康的持续时间
func (ds *DataServerInfo) GetUnhealthyDuration() time.Duration {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()

	if ds.UnhealthyStartTime == nil {
		return 0
	}
	return time.Since(*ds.UnhealthyStartTime)
}

// RecoverToHealthy 恢复节点为健康状态
func (ds *DataServerInfo) RecoverToHealthy() {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	ds.IsHealthy = true
	ds.UnhealthyStartTime = nil
	ds.IsPermanentlyDown = false
}

// BlockMapping 表示数据块在各个 DataServer 上的分布
type BlockMapping struct {
	BlockID   uint64   // 数据块 ID
	Locations []string // 存储该块的 DataServer 地址列表
}

// BlockWithLocations 表示带有位置信息的数据块
type BlockWithLocations struct {
	BlockID   uint64   // 数据块 ID
	Locations []string // 存储该块的 DataServer 地址列表
}

// GCEntry 表示待垃圾回收的数据块
type GCEntry struct {
	BlockID    uint64    // 待删除的块 ID
	DeleteTime time.Time // 删除时间戳
	Locations  []string  // 需要从哪些 DataServer 删除
	Status     string    // GC 状态: "pending", "sent", "completed"
	SentTime   time.Time // 删除命令发送时间
}

// Command 表示需要下发给 DataServer 的指令
type Command struct {
	Action  string   // 动作类型: "DELETE_BLOCK" 或 "COPY_BLOCK"
	BlockID uint64   // 目标块 ID
	Targets []string // 目标地址列表
}

// RepairTask 修复任务结构体
type RepairTask struct {
	BlockID           uint64   // 块ID
	SourceAddr        string   // 源地址
	TargetAddrs       []string // 目标地址列表
	IsReplacement     bool     // 是否为替换操作
	ReplacedAddr      string   // 被替换的地址
	ExpectedLocations []string // 期望位置列表
	Priority          int      // 优先级 (1=高, 2=中, 3=低)
}

// FSCKCheckTask FSCK检查任务
type FSCKCheckTask struct {
	BlockID           uint64   // 块ID
	ExpectedLocations []string // 期望位置
	ActualLocations   []string // 实际位置
}

// BadgerDB Key Prefixes
const (
	PrefixInode   = "i/"  // 存储 NodeInfo
	PrefixPath    = "p/"  // 路径到 Inode ID 的映射
	PrefixDir     = "d/"  // 目录条目
	PrefixBlock   = "b/"  // 块映射
	PrefixGC      = "gc/" // 垃圾回收
	PrefixCounter = "c/"  // 计数器 (如 Inode ID 生成器)
)

// InodeCounter 用于生成唯一的 Inode ID
const InodeCounterKey = PrefixCounter + "inode"

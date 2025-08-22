package model

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

const (
	DefaultBlockSize = 4 * 1024 * 1024 // 4MB per block
)

// Config 配置结构体，映射 config.yaml
type Config struct {
	Server struct {
		ListenAddress string `yaml:"listen_address"`
		DataserverId  string `yaml:"dataServer_id"`
	} `yaml:"server"`

	Storage struct {
		DataRootPath   string `yaml:"data_root_path"`
		MaxStorageSize uint64 `yaml:"max_storage_size"`
		BlockSize      uint64 `yaml:"block_size"`
	} `yaml:"storage"`

	Etcd struct {
		Endpoints   []string `yaml:"endpoints"`
		DialTimeout int      `yaml:"dial_timeout"`
		LeaseTTL    int64    `yaml:"lease_ttl"`
	} `yaml:"etcd"`

	MetaServer struct {
		// Address removed - now uses etcd leader discovery
		HeartbeatInterval int `yaml:"heartbeat_interval"`
		ConnectionTimeout int `yaml:"connection_timeout"`
	} `yaml:"metaServer"`

	Logging struct {
		Level  string `yaml:"level"`
		Output string `yaml:"output"`
	} `yaml:"logging"`
}

// DataServer 核心状态结构体
type DataServer struct {
	Config     *Config
	GrpcServer *grpc.Server
	EtcdClient *clientv3.Client
	MetaClient *grpc.ClientConn

	// 服务组件
	StorageService     *StorageService
	ReplicationService *ReplicationService
	ClusterService     *ClusterService

	// 运行时状态
	IsRunning bool
	StopChan  chan struct{}
}

// StorageService 存储服务接口
type StorageService interface {
	WriteBlock(blockID uint64, data []byte) error
	ReadBlock(blockID uint64) ([]byte, error)
	DeleteBlock(blockID uint64) error
	GetStat() (*StorageStat, error)
	BlockExists(blockID uint64) bool
	ListBlocks() ([]uint64, error)
}

// ReplicationService 复制服务接口
type ReplicationService interface {
	ForwardBlock(targetAddr string, metadata *WriteBlockMetadata, data []byte) error
	PushBlock(targetAddr string, blockID uint64, data []byte) error
	PullBlock(sourceAddr string, blockID uint64) ([]byte, error)
}

// ClusterService 集群服务接口
type ClusterService interface {
	RegisterToETCD() error
	StartHeartbeatLoop() error
	Stop() error
}

// StorageStat 存储统计信息
type StorageStat struct {
	BlockCount    uint64
	FreeSpace     uint64
	UsedSpace     uint64
	TotalCapacity uint64
	BlockIds      []uint64
}

// WriteBlockMetadata 写入块元数据
type WriteBlockMetadata struct {
	BlockId          uint64
	ReplicaLocations []string
}

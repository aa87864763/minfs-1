package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"metaServer/internal/handler"
	"metaServer/internal/model"
	"metaServer/internal/service"
	"metaServer/pb"

	"github.com/dgraph-io/badger/v3"
	hashiraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
	port       = flag.Int("port", 9090, "gRPC server port")
	raftPort   = flag.Int("raft-port", 10090, "Raft server port")
	nodeID     = flag.String("node-id", "", "MetaServer node ID")
	dataDir    = flag.String("data-dir", "", "BadgerDB data directory")
	bootstrap  = flag.Bool("bootstrap", false, "Bootstrap new Raft cluster")
)

func main() {
	flag.Parse()

	log.Println("Starting MetaServer with Raft consensus...")

	// 加载配置
	config, err := model.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 如果命令行指定了端口，覆盖配置文件
	if *port != 9090 {
		config.Server.GrpcPort = *port
	}
	
	// 如果命令行指定了数据目录，覆盖配置文件
	if *dataDir != "" {
		config.Database.BadgerDir = *dataDir
	}
	
	// 生成节点ID
	var currentNodeID string
	if *nodeID != "" {
		currentNodeID = *nodeID
	} else {
		currentNodeID = fmt.Sprintf("meta-%d", config.Server.GrpcPort)
	}

	// Raft 地址
	raftAddr := fmt.Sprintf("localhost:%d", *raftPort)

	log.Printf("Configuration: NodeID=%s, gRPC=%d, Raft=%d, Data=%s", 
		currentNodeID, config.Server.GrpcPort, *raftPort, config.Database.BadgerDir)

	// 初始化 BadgerDB
	db, err := initBadgerDB(config.Database.BadgerDir)
	if err != nil {
		log.Fatalf("Failed to initialize BadgerDB: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing BadgerDB: %v", err)
		}
	}()

	// 初始化 Raft 服务
	raftService, err := service.NewRaftService(db, config, currentNodeID, raftAddr)
	if err != nil {
		log.Fatalf("Failed to initialize Raft: %v", err)
	}
	defer raftService.Shutdown()

	// Bootstrap 集群（如果指定）
	if *bootstrap {
		log.Printf("Bootstrapping Raft cluster with all configured peers...")
		
		// 从配置文件构建所有节点的 Server 列表
		// 注意：所有节点必须同时启动才能形成 quorum
		servers := make([]hashiraft.Server, 0, len(config.Raft.Peers))
		for _, peer := range config.Raft.Peers {
			servers = append(servers, hashiraft.Server{
				ID:      hashiraft.ServerID(peer.ID),
				Address: hashiraft.ServerAddress(peer.RaftAddr),
			})
		}
		
		log.Printf("Bootstrap with %d nodes: %v", len(servers), servers)
		if err := raftService.Bootstrap(servers); err != nil {
			log.Fatalf("Bootstrap failed: %v", err)
		}
	}

	// 等待 Leader 选举
	log.Printf("Waiting for leader election...")
	if err := raftService.WaitForLeader(30 * time.Second); err != nil {
		log.Fatalf("Leader election failed: %v", err)
	}
	
	isLeader := raftService.IsLeader()
	if isLeader {
		log.Printf("✓ This node is LEADER")
	} else {
		log.Printf("✓ Leader: %s", raftService.GetLeaderAddr())
	}

	// 初始化 etcd 服务（用于 DataServer 发现和 MetaServer 注册）
	etcdService, err := service.NewEtcdService(config)
	if err != nil {
		log.Fatalf("Failed to init etcd service: %v", err)
	}
	defer etcdService.Stop()

	// 注册本节点到 etcd（用于服务发现）
	nodeAddr := fmt.Sprintf("localhost:%d", config.Server.GrpcPort)
	if err := etcdService.RegisterMetaServer(currentNodeID, nodeAddr, isLeader); err != nil {
		log.Fatalf("Failed to register MetaServer: %v", err)
	}
	defer etcdService.UnregisterMetaServer(currentNodeID)

	// 监听 Leader 变化并更新 etcd
	go monitorLeaderChanges(raftService, etcdService, currentNodeID, nodeAddr)

	// 初始化根目录
	if err := initRootDirectory(db, config); err != nil {
		log.Fatalf("Failed to init root: %v", err)
	}

	// 初始化服务层
	metadataService := service.NewMetadataService(db, config, raftService)
	clusterService := service.NewClusterService(config)
	schedulerService := service.NewSchedulerService(config, clusterService, metadataService)
	clusterService.SetRaftService(raftService)
	clusterService.SetEtcdService(etcdService)

	// 初始化 gRPC Handler
	metaHandler := handler.NewMetaServerHandler(metadataService, clusterService, schedulerService)

	// 启动 gRPC 服务器
	grpcServer := grpc.NewServer()
	pb.RegisterMetaServerServiceServer(grpcServer, metaHandler)

	addr := fmt.Sprintf(":%d", config.Server.GrpcPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("MetaServer ready on %s (Raft: %s)", addr, raftAddr)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("gRPC serve failed: %v", err)
		}
	}()

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("Received signal: %v", sig)

	// 优雅关闭
	log.Println("Shutting down...")
	grpcServer.GracefulStop()
	schedulerService.Stop()
	clusterService.Stop()
	raftService.Shutdown()
	log.Println("Shutdown complete")
}

// monitorLeaderChanges 监听 Raft Leader 变化并更新 etcd
func monitorLeaderChanges(
	raftService *service.RaftService,
	etcdService *service.EtcdService,
	nodeID, nodeAddr string,
) {
	leaderCh := raftService.LeaderCh()
	for isLeader := range leaderCh {
		if isLeader {
			log.Printf(">>> This node became LEADER <<<")
		} else {
			log.Printf(">>> This node became FOLLOWER <<<")
			leaderAddr := raftService.GetLeaderAddr()
			if leaderAddr != "" {
				log.Printf(">>> New leader: %s <<<", leaderAddr)
			}
		}
		// 更新 etcd 中的 Leader 状态
		if err := etcdService.UpdateMetaServerLeaderStatus(nodeID, nodeAddr, isLeader); err != nil {
			log.Printf("Failed to update leader status in etcd: %v", err)
		}
	}
}

// initBadgerDB 初始化 BadgerDB
func initBadgerDB(dbDir string) (*badger.DB, error) {
	// 确保数据库目录存在
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %v", err)
	}

	// BadgerDB 选项
	opts := badger.DefaultOptions(dbDir)
	opts.Logger = nil // 禁用 BadgerDB 的日志输出

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %v", err)
	}

	log.Printf("BadgerDB initialized at: %s", dbDir)
	return db, nil
}

// initRootDirectory 初始化根目录
func initRootDirectory(db *badger.DB, config *model.Config) error {
	// 根目录初始化不需要 Raft 服务，传递 nil
	metadataService := service.NewMetadataService(db, config, nil)
	
	// 检查根目录是否存在
	_, err := metadataService.GetNodeInfo("/")
	if err == nil {
		log.Println("Root directory already exists")
		return nil
	}

	// 创建根目录（直接写数据库，不通过 Raft）
	err = metadataService.CreateNode("/", pb.FileType_Directory)
	if err != nil {
		return fmt.Errorf("failed to create root directory: %v", err)
	}

	log.Println("Root directory created")
	return nil
}
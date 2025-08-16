package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"metaserver/internal/handler"
	"metaserver/internal/model"
	"metaserver/internal/service"
	"metaserver/pb"

	"github.com/dgraph-io/badger/v3"
	"google.golang.org/grpc"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
	port       = flag.Int("port", 9090, "gRPC server port")
	nodeID     = flag.String("node-id", "", "MetaServer node ID (auto-generated if not provided)")
	dataDir    = flag.String("data-dir", "", "BadgerDB data directory (overrides config)")
)

func main() {
	flag.Parse()

	log.Println("Starting MetaServer...")

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
		currentNodeID = fmt.Sprintf("metaserver-%d", config.Server.GrpcPort)
	}

	log.Printf("Configuration loaded: NodeID=%s, gRPC port=%d, BadgerDB dir=%s", 
		currentNodeID, config.Server.GrpcPort, config.Database.BadgerDir)

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

	// 初始化根目录（如果不存在）
	if err := initRootDirectory(db, config); err != nil {
		log.Fatalf("Failed to initialize root directory: %v", err)
	}

	// 初始化 WAL 服务
	walService := service.NewWALService(db, config, currentNodeID)
	
	// 初始化服务层
	metadataService := service.NewMetadataService(db, config, walService)
	clusterService := service.NewClusterService(config)
	schedulerService := service.NewSchedulerService(config, clusterService, metadataService)
	
	// 初始化Leader Election服务
	nodeAddr := fmt.Sprintf("localhost:%d", config.Server.GrpcPort)
	leaderElection, err := service.NewLeaderElection(config, currentNodeID, nodeAddr)
	if err != nil {
		log.Fatalf("Failed to initialize leader election: %v", err)
	}
	
	// 设置Leader Election和WAL服务的相互引用
	leaderElection.SetWALService(walService)
	walService.SetLeaderElection(leaderElection)
	clusterService.SetLeaderElection(leaderElection) // 传递选举服务
	
	log.Printf("Node %s initialized and ready to start", currentNodeID)
	
	// 启动Leader Election
	if err := leaderElection.Start(); err != nil {
		log.Fatalf("Failed to start leader election: %v", err)
	}
	
	// 回放WAL日志以恢复状态
	if err := replayWALLogs(walService, metadataService); err != nil {
		log.Printf("Warning: Failed to replay WAL logs: %v", err)
	}

	// 初始化 gRPC Handler
	metaHandler := handler.NewMetaServerHandler(metadataService, clusterService, schedulerService)
	metaHandler.SetWALService(walService) // 设置WAL服务

	// 启动 gRPC 服务器
	grpcServer := grpc.NewServer()
	pb.RegisterMetaServerServiceServer(grpcServer, metaHandler)

	// 监听端口
	addr := fmt.Sprintf(":%d", config.Server.GrpcPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", addr, err)
	}

	// 启动服务器
	log.Printf("MetaServer listening on %s", addr)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// 等待信号来优雅关闭
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 阻塞直到收到信号
	sig := <-sigChan
	log.Printf("Received signal: %v", sig)

	// 优雅关闭
	log.Println("Shutting down MetaServer...")
	
	// 停止 gRPC 服务器
	grpcServer.GracefulStop()
	
	// 停止后台服务
	schedulerService.Stop()
	clusterService.Stop()
	leaderElection.Stop()
	walService.Close()

	log.Println("MetaServer shutdown complete")
}

// replayWALLogs 回放WAL日志
func replayWALLogs(walService *service.WALService, metadataService *service.MetadataService) error {
	log.Println("Starting WAL replay...")
	
	// 获取当前最大日志索引
	currentIndex := walService.GetCurrentLogIndex()
	if currentIndex == 0 {
		log.Println("No WAL entries to replay")
		return nil
	}
	
	// 从索引1开始回放所有日志
	entries, err := walService.GetLogEntriesFrom(1, int(currentIndex))
	if err != nil {
		return fmt.Errorf("failed to get WAL entries: %v", err)
	}
	
	replayedCount := 0
	for _, entry := range entries {
		err := walService.ReplayLogEntry(entry, metadataService)
		if err != nil {
			log.Printf("Failed to replay WAL entry %d: %v", entry.LogIndex, err)
			// 继续回放其他条目，不要因为一个失败就停止
			continue
		}
		replayedCount++
	}
	
	log.Printf("WAL replay completed: %d/%d entries replayed successfully", 
		replayedCount, len(entries))
	return nil
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
	// 根目录初始化不需要WAL服务，传递nil
	metadataService := service.NewMetadataService(db, config, nil)
	
	// 检查根目录是否存在
	_, err := metadataService.GetNodeInfo("/")
	if err == nil {
		// 根目录已存在
		log.Println("Root directory already exists")
		return nil
	}

	// 创建根目录
	err = metadataService.CreateNode("/", pb.FileType_Directory)
	if err != nil {
		return fmt.Errorf("failed to create root directory: %v", err)
	}

	log.Println("Root directory created successfully")
	return nil
}
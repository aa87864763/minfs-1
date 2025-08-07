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

	log.Printf("Configuration loaded: gRPC port=%d, BadgerDB dir=%s", 
		config.Server.GrpcPort, config.Database.BadgerDir)

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

	// 初始化服务层
	metadataService := service.NewMetadataService(db, config)
	clusterService := service.NewClusterService(config)
	schedulerService := service.NewSchedulerService(config, clusterService, metadataService)

	// 初始化 gRPC Handler
	metaHandler := handler.NewMetaServerHandler(metadataService, clusterService, schedulerService)

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

	log.Println("MetaServer shutdown complete")
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
	metadataService := service.NewMetadataService(db, config)
	
	// 检查根目录是否存在
	_, err := metadataService.GetNodeInfo("/")
	if err == nil {
		// 根目录已存在
		log.Println("Root directory already exists")
		return nil
	}

	// 创建根目录
	err = metadataService.CreateNode("/", pb.NodeType_DIRECTORY)
	if err != nil {
		return fmt.Errorf("failed to create root directory: %v", err)
	}

	log.Println("Root directory created successfully")
	return nil
}
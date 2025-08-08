package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"dataserver/internal/handler"
	"dataserver/internal/model"
	"dataserver/internal/service"
	"dataserver/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gopkg.in/yaml.v3"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
	mockMode   = flag.Bool("mock", false, "Run in mock mode without etcd dependency")
)

func main() {
	flag.Parse()
	
	// 加载配置
	config, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	
	log.Printf("Starting DataServer %s (Mock Mode: %v)", config.Server.DataserverId, *mockMode)
	log.Printf("Listening on %s", config.Server.ListenAddress)
	
	// 初始化存储服务
	storageService, err := service.NewStorageService(config.Storage.DataRootPath)
	if err != nil {
		log.Fatalf("Failed to create storage service: %v", err)
	}
	log.Printf("Storage service initialized with root path: %s", config.Storage.DataRootPath)
	
	// 初始化复制服务
	replicationService := service.NewReplicationService()
	defer replicationService.Close()
	log.Println("Replication service initialized")
	
	// 初始化集群服务 - 根据模式选择实现
	var clusterService model.ClusterService
	if *mockMode {
		clusterService = service.NewMockClusterService(config, storageService)
		log.Println("Mock cluster service initialized")
	} else {
		clusterService, err = service.NewClusterService(config, storageService)
		if err != nil {
			log.Fatalf("Failed to create cluster service: %v", err)
		}
		log.Println("Real cluster service initialized")
	}
	defer clusterService.Stop()
	
	// 注册到etcd (或模拟注册)
	if err := clusterService.RegisterToETCD(); err != nil {
		log.Fatalf("Failed to register to etcd: %v", err)
	}
	
	// 启动心跳循环
	if err := clusterService.StartHeartbeatLoop(); err != nil {
		log.Fatalf("Failed to start heartbeat loop: %v", err)
	}
	
	// 创建gRPC处理器
	grpcHandler := handler.NewDataServerHandler(storageService, replicationService)
	log.Println("gRPC handler created")
	
	// 创建gRPC服务器
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*1024), // 1GB
		grpc.MaxSendMsgSize(1024*1024*1024), // 1GB
	)
	
	// 注册服务
	pb.RegisterDataServerServiceServer(grpcServer, grpcHandler)
	
	// 启用反射（便于调试）
	reflection.Register(grpcServer)
	
	// 创建监听器
	listener, err := net.Listen("tcp", config.Server.ListenAddress)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()
	
	if *mockMode {
		log.Printf("DataServer listening on %s (MOCK MODE - No etcd required)", config.Server.ListenAddress)
	} else {
		log.Printf("DataServer listening on %s", config.Server.ListenAddress)
	}
	
	// 启动gRPC服务器（在goroutine中）
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()
	
	// 等待中断信号
	waitForShutdown(grpcServer, clusterService)
	
	log.Println("DataServer shutdown complete")
}

// loadConfig 加载配置文件
func loadConfig(configPath string) (*model.Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}
	
	var config model.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	
	// 验证配置
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	
	return &config, nil
}

// validateConfig 验证配置的有效性
func validateConfig(config *model.Config) error {
	if config.Server.ListenAddress == "" {
		return fmt.Errorf("server.listen_address is required")
	}
	
	if config.Server.DataserverId == "" {
		return fmt.Errorf("server.dataserver_id is required")
	}
	
	if config.Storage.DataRootPath == "" {
		return fmt.Errorf("storage.data_root_path is required")
	}
	
	if len(config.Etcd.Endpoints) == 0 {
		return fmt.Errorf("etcd.endpoints is required")
	}
	
	if config.MetaServer.Address == "" {
		return fmt.Errorf("metaserver.address is required")
	}
	
	// 设置默认值
	if config.Etcd.DialTimeout == 0 {
		config.Etcd.DialTimeout = 5
	}
	
	if config.Etcd.LeaseTTL == 0 {
		config.Etcd.LeaseTTL = 30
	}
	
	if config.MetaServer.HeartbeatInterval == 0 {
		config.MetaServer.HeartbeatInterval = 10
	}
	
	if config.MetaServer.ConnectionTimeout == 0 {
		config.MetaServer.ConnectionTimeout = 5
	}
	
	if config.Storage.BlockSize == 0 {
		config.Storage.BlockSize = 4 * 1024 * 1024 // 4MB default
	}
	
	return nil
}

// waitForShutdown 等待关闭信号并优雅关闭
func waitForShutdown(grpcServer *grpc.Server, clusterService model.ClusterService) {
	// 创建信号通道
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// 等待信号
	sig := <-sigChan
	log.Printf("Received signal %s, initiating graceful shutdown...", sig)
	
	// 停止接受新的连接
	grpcServer.GracefulStop()
	log.Println("gRPC server stopped")
	
	// 停止集群服务
	if err := clusterService.Stop(); err != nil {
		log.Printf("Error stopping cluster service: %v", err)
	}
	log.Println("Cluster service stopped")
}

// printStartupBanner 打印启动横幅
func printStartupBanner(config *model.Config) {
	banner := `
╔═══════════════════════════════════════════════════╗
║                  Mini-DFS DataServer              ║
║                                                   ║
║  A distributed file system data storage node     ║
╚═══════════════════════════════════════════════════╝
`
	fmt.Println(banner)
	
	log.Printf("Server ID: %s", config.Server.DataserverId)
	log.Printf("Listen Address: %s", config.Server.ListenAddress)
	log.Printf("Data Root Path: %s", config.Storage.DataRootPath)
	log.Printf("Etcd Endpoints: %v", config.Etcd.Endpoints)
	log.Printf("MetaServer Address: %s", config.MetaServer.Address)
	fmt.Println()
}
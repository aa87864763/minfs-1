package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"metaServer/internal/handler"
	"metaServer/internal/model"
	"metaServer/internal/service"
	"metaServer/pb"

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
		currentNodeID = fmt.Sprintf("metaServer-%d", config.Server.GrpcPort)
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
	
	// 先设置所有回调函数
	// 设置leader变化回调
	leaderElection.SetLeaderChangeCallback(func(isLeader bool) {
		if isLeader {
			log.Printf("Node %s became leader, replaying WAL logs", currentNodeID)
			if err := replayWALLogs(walService, metadataService); err != nil {
				log.Printf("Warning: Failed to replay WAL logs: %v", err)
			}
		}
	})
	
	// 设置WAL同步回调（当发现新leader时触发）
	leaderElection.SetWALSyncCallback(func() {
		log.Printf("Node %s triggered WAL sync from leader", currentNodeID)
		if err := requestWALSyncFromLeader(walService, leaderElection, currentNodeID); err != nil {
			log.Printf("Warning: Failed to sync WAL from leader: %v", err)
		} else {
			leaderElection.MarkWALSyncCompleted()
			log.Printf("Node %s completed WAL sync from leader", currentNodeID)
		}
	})
	
	// 简化启动逻辑：所有节点都以follower模式启动
	// RegisterAsFollower会自动参与选举，如果没有leader会自动成为leader
	log.Printf("Node %s starting in follower mode (will auto-elect if no leader)", currentNodeID)
	
	if err := leaderElection.RegisterAsFollower(); err != nil {
		log.Fatalf("Failed to register as follower: %v", err)
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
	
	// 使用WaitGroup和超时机制保护关闭流程
	var wg sync.WaitGroup
	shutdownTimeout := 30 * time.Second
	shutdownComplete := make(chan struct{})
	
	go func() {
		defer close(shutdownComplete)
		
		// 主动注销etcd注册信息 (带超时保护)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := gracefulDeregisterFromETCD(leaderElection, currentNodeID); err != nil {
				log.Printf("Failed to deregister from etcd: %v", err)
			}
		}()
		
		// 等待etcd注销完成或超时
		etcdDone := make(chan struct{})
		go func() {
			wg.Wait()
			close(etcdDone)
		}()
		
		select {
		case <-etcdDone:
			log.Println("Etcd deregistration completed")
		case <-time.After(10 * time.Second):
			log.Println("Etcd deregistration timeout, proceeding with shutdown")
		}
		
		// 停止 gRPC 服务器
		grpcServer.GracefulStop()
		
		// 停止后台服务
		schedulerService.Stop()
		clusterService.Stop()
		leaderElection.Stop()
		walService.Close()
	}()
	
	// 等待关闭完成或超时
	select {
	case <-shutdownComplete:
		log.Println("MetaServer shutdown complete")
	case <-time.After(shutdownTimeout):
		log.Println("Shutdown timeout reached, forcing exit")
		grpcServer.Stop() // 强制停止gRPC服务器
	}
}

// checkExistingLeader 检查是否已存在leader（直接从etcd查询，避免竞态条件）
func checkExistingLeader(leaderElection *service.LeaderElection) (bool, error) {
	log.Println("Checking for existing leader directly from etcd...")
	
	// 直接从etcd查询，不依赖本地缓存
	hasLeader := leaderElection.CheckLeaderDirectly()
	if hasLeader {
		log.Println("Found existing leader in etcd")
		return true, nil
	}
	
	log.Println("No existing leader found in etcd")
	return false, nil
}

// requestWALSyncFromLeader 从leader请求WAL同步
func requestWALSyncFromLeader(walService *service.WALService, leaderElection *service.LeaderElection, nodeID string) error {
	log.Println("Requesting WAL sync from leader...")
	
	// 获取当前leader信息
	leaderInfo := leaderElection.GetCurrentLeader()
	if leaderInfo == nil {
		return fmt.Errorf("no leader available")
	}
	
	leaderAddr := fmt.Sprintf("%s:%d", leaderInfo.Host, leaderInfo.Port)
	
	// 获取本地最后的日志索引
	lastLogIndex := walService.GetCurrentLogIndex()
	
	log.Printf("Requesting WAL sync from leader %s, last local index: %d", leaderAddr, lastLogIndex)
	
	// 请求WAL同步
	err := walService.RequestWALSyncFromLeader(leaderAddr, nodeID, lastLogIndex)
	if err != nil {
		return fmt.Errorf("failed to sync WAL from leader: %v", err)
	}
	
	log.Println("WAL sync from leader completed successfully")
	return nil
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

// gracefulDeregisterFromETCD 优雅地从etcd注销服务信息 (带保护机制)
func gracefulDeregisterFromETCD(leaderElection *service.LeaderElection, nodeID string) error {
	log.Printf("Attempting to gracefully deregister MetaServer %s from etcd...", nodeID)
	
	// 使用sync.Once确保只执行一次
	var once sync.Once
	var finalErr error
	
	once.Do(func() {
		// 设置总体超时
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		// 获取etcd客户端
		etcdClient, err := leaderElection.GetETCDClient()
		if err != nil {
			finalErr = fmt.Errorf("failed to get etcd client: %v", err)
			return
		}
		
		// 删除节点注册信息 (带重试机制)
		nodeKey := fmt.Sprintf("/minfs/metaServer/nodes/%s", nodeID)
		retryCount := 3
		
		for i := 0; i < retryCount; i++ {
			if ctx.Err() != nil {
				finalErr = fmt.Errorf("context timeout during node deletion")
				return
			}
			
			if _, err := etcdClient.Delete(ctx, nodeKey); err != nil {
				log.Printf("Failed to delete node key %s (attempt %d/%d): %v", nodeKey, i+1, retryCount, err)
				if i == retryCount-1 {
					log.Printf("All retry attempts failed for node key deletion")
				} else {
					time.Sleep(time.Duration(i+1) * time.Second) // 指数退避
				}
			} else {
				log.Printf("Successfully deleted node key: %s", nodeKey)
				break
			}
		}
		
		// 如果是leader，主动resign (带超时保护)
		if leaderElection.IsLeader() {
			log.Printf("Node %s is leader, resigning from leadership...", nodeID)
			resignDone := make(chan error, 1)
			
			go func() {
				resignDone <- leaderElection.Resign()
			}()
			
			select {
			case err := <-resignDone:
				if err != nil {
					log.Printf("Failed to resign leadership: %v", err)
				} else {
					log.Printf("Successfully resigned from leadership")
				}
			case <-time.After(5 * time.Second):
				log.Printf("Leadership resignation timeout")
			case <-ctx.Done():
				log.Printf("Context timeout during leadership resignation")
			}
		}
	})
	
	if finalErr != nil {
		return finalErr
	}
	
	log.Printf("MetaServer %s successfully deregistered from etcd", nodeID)
	return nil
}
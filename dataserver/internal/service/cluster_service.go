package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"dataserver/internal/model"
	"dataserver/pb"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EtcdClusterService etcd集群服务实现
type EtcdClusterService struct {
	config         *model.Config
	etcdClient     *clientv3.Client
	metaClient     *grpc.ClientConn
	storageService model.StorageService
	
	// 租约管理
	lease          clientv3.Lease
	leaseID        clientv3.LeaseID
	
	// 控制循环
	stopChan       chan struct{}
	isRunning      bool
}

// NewClusterService 创建集群服务实例
func NewClusterService(config *model.Config, storageService model.StorageService) (*EtcdClusterService, error) {
	// 创建etcd客户端
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Etcd.Endpoints,
		DialTimeout: time.Duration(config.Etcd.DialTimeout) * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}
	
	// 创建metaserver连接
	ctx, cancel := context.WithTimeout(context.Background(), 
		time.Duration(config.MetaServer.ConnectionTimeout)*time.Second)
	defer cancel()
	
	metaConn, err := grpc.DialContext(ctx, config.MetaServer.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		etcdClient.Close()
		return nil, fmt.Errorf("failed to connect to metaserver: %w", err)
	}
	
	return &EtcdClusterService{
		config:         config,
		etcdClient:     etcdClient,
		metaClient:     metaConn,
		storageService: storageService,
		lease:          clientv3.NewLease(etcdClient),
		stopChan:       make(chan struct{}),
	}, nil
}

// RegisterToETCD 在etcd中注册本服务
func (s *EtcdClusterService) RegisterToETCD() error {
	ctx := context.Background()
	
	// 创建租约
	ttl := s.config.Etcd.LeaseTTL
	leaseResp, err := s.lease.Grant(ctx, ttl)
	if err != nil {
		return fmt.Errorf("failed to grant lease: %w", err)
	}
	
	s.leaseID = leaseResp.ID
	
	// 注册服务key
	key := fmt.Sprintf("/dfs/dataserver/%s", s.config.Server.DataserverId)
	value := fmt.Sprintf("%s", s.config.Server.ListenAddress)
	
	_, err = s.etcdClient.Put(ctx, key, value, clientv3.WithLease(s.leaseID))
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}
	
	// 启动租约续期
	ch, kaerr := s.lease.KeepAlive(ctx, s.leaseID)
	if kaerr != nil {
		return fmt.Errorf("failed to keep alive lease: %w", kaerr)
	}
	
	// 启动后台goroutine处理租约续期响应
	go func() {
		for ka := range ch {
			if ka == nil {
				log.Println("Lease keep-alive channel closed")
				return
			}
			// 可以在这里记录日志或处理续期响应
		}
	}()
	
	log.Printf("Successfully registered to etcd: %s -> %s", key, value)
	return nil
}

// StartHeartbeatLoop 启动心跳循环
func (s *EtcdClusterService) StartHeartbeatLoop() error {
	if s.isRunning {
		return fmt.Errorf("heartbeat loop is already running")
	}
	
	s.isRunning = true
	
	// 启动心跳goroutine
	go s.heartbeatLoop()
	
	log.Println("Heartbeat loop started")
	return nil
}

// Stop 停止集群服务
func (s *EtcdClusterService) Stop() error {
	if !s.isRunning {
		return nil
	}
	
	// 停止心跳循环
	close(s.stopChan)
	s.isRunning = false
	
	// 撤销租约
	if s.leaseID != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		_, err := s.lease.Revoke(ctx, s.leaseID)
		if err != nil {
			log.Printf("Failed to revoke lease: %v", err)
		}
	}
	
	// 关闭连接
	if s.metaClient != nil {
		s.metaClient.Close()
	}
	
	if s.etcdClient != nil {
		s.etcdClient.Close()
	}
	
	log.Println("Cluster service stopped")
	return nil
}

// heartbeatLoop 心跳循环实现
func (s *EtcdClusterService) heartbeatLoop() {
	ticker := time.NewTicker(time.Duration(s.config.MetaServer.HeartbeatInterval) * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := s.sendHeartbeat(); err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
			}
			
		case <-s.stopChan:
			log.Println("Heartbeat loop stopping")
			return
		}
	}
}

// sendHeartbeat 发送心跳到metaserver
func (s *EtcdClusterService) sendHeartbeat() error {
	// 获取存储统计
	stat, err := s.storageService.GetStat()
	if err != nil {
		return fmt.Errorf("failed to get storage stat: %w", err)
	}
	
	// 创建metaserver客户端
	client := NewMetaServerServiceClient(s.metaClient)
	
	// 构建心跳请求
	req := &pb.HeartbeatRequest{
		DataserverId:   s.config.Server.DataserverId,
		DataserverAddr: s.config.Server.ListenAddress,
		BlockCount:     stat.BlockCount,
		FreeSpace:      stat.FreeSpace,
		BlockIdsReport: stat.BlockIds,
	}
	
	// 打印心跳请求数据到控制台
	log.Printf("📡 [HEARTBEAT REQUEST] DataServer: %s", req.DataserverId)
	log.Printf("    └── Address: %s", req.DataserverAddr)
	log.Printf("    └── Block Count: %d", req.BlockCount)
	log.Printf("    └── Free Space: %d bytes (%.2f MB)", req.FreeSpace, float64(req.FreeSpace)/(1024*1024))
	if len(req.BlockIdsReport) > 0 {
		if len(req.BlockIdsReport) <= 10 {
			log.Printf("    └── Block IDs: %v", req.BlockIdsReport)
		} else {
			log.Printf("    └── Block IDs: %v... (total: %d blocks)", req.BlockIdsReport[:10], len(req.BlockIdsReport))
		}
	} else {
		log.Printf("    └── Block IDs: [] (no blocks stored)")
	}
	
	// 发送心跳
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	resp, err := client.Heartbeat(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send heartbeat: %w", err)
	}
	
	// 打印心跳响应数据到控制台
	log.Printf("💓 [HEARTBEAT RESPONSE] Commands received: %d", len(resp.Commands))
	if len(resp.Commands) > 0 {
		for i, cmd := range resp.Commands {
			actionName := "UNKNOWN"
			switch cmd.Action {
			case pb.Command_DELETE_BLOCK:
				actionName = "DELETE_BLOCK"
			case pb.Command_COPY_BLOCK:
				actionName = "COPY_BLOCK"
			}
			log.Printf("    └── Command %d: %s (Block ID: %d)", i+1, actionName, cmd.BlockId)
			if len(cmd.Targets) > 0 {
				log.Printf("        └── Targets: %v", cmd.Targets)
			}
		}
		go s.processCommands(resp.Commands)
	} else {
		log.Printf("    └── No commands from MetaServer")
	}
	
	return nil
}

// processCommands 处理来自metaserver的命令
func (s *EtcdClusterService) processCommands(commands []*pb.Command) {
	for _, cmd := range commands {
		if err := s.processCommand(cmd); err != nil {
			log.Printf("Failed to process command: %v", err)
		}
	}
}

// processCommand 处理单个命令
func (s *EtcdClusterService) processCommand(cmd *pb.Command) error {
	switch cmd.Action {
	case pb.Command_DELETE_BLOCK:
		return s.processDeleteCommand(cmd.BlockId)
		
	case pb.Command_COPY_BLOCK:
		return s.processReplicateCommand(cmd.BlockId, cmd.Targets)
		
	default:
		return fmt.Errorf("unknown command action: %d", cmd.Action)
	}
}

// processDeleteCommand 处理删除块命令
func (s *EtcdClusterService) processDeleteCommand(blockID uint64) error {
	log.Printf("Processing delete command for block %d", blockID)
	
	if err := s.storageService.DeleteBlock(blockID); err != nil {
		return fmt.Errorf("failed to delete block %d: %w", blockID, err)
	}
	
	log.Printf("Successfully deleted block %d", blockID)
	return nil
}

// processReplicateCommand 处理复制块命令 - 从源地址复制数据到本地
func (s *EtcdClusterService) processReplicateCommand(blockID uint64, targets []string) error {
	if len(targets) == 0 {
		return fmt.Errorf("no source address provided for block %d replication", blockID)
	}
	
	sourceAddr := targets[0] // targets[0] 是源地址
	log.Printf("Processing replicate command for block %d from source: %s", blockID, sourceAddr)
	
	// 连接到源DataServer
	conn, err := grpc.Dial(sourceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to source %s: %w", sourceAddr, err)
	}
	defer conn.Close()
	
	client := pb.NewDataServerServiceClient(conn)
	
	// 从源地址读取块数据
	req := &pb.ReadBlockRequest{
		BlockId: blockID,
	}
	
	stream, err := client.ReadBlock(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to read block %d from source %s: %w", blockID, sourceAddr, err)
	}
	
	var blockData []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive block data: %w", err)
		}
		blockData = append(blockData, resp.ChunkData...)
	}
	
	// 将数据写入本地存储
	if err := s.storageService.WriteBlock(blockID, blockData); err != nil {
		return fmt.Errorf("failed to write block %d locally: %w", blockID, err)
	}
	
	log.Printf("Successfully replicated block %d from %s (%d bytes)", blockID, sourceAddr, len(blockData))
	return nil
}

// 简化的MetaServer客户端实现
type metaServerClient struct {
	conn *grpc.ClientConn
}

func NewMetaServerServiceClient(conn *grpc.ClientConn) pb.MetaServerServiceClient {
	return &metaServerClient{conn: conn}
}

func (c *metaServerClient) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest, opts ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
	resp := &pb.HeartbeatResponse{}
	err := c.conn.Invoke(ctx, "/dfs_project.MetaServerService/Heartbeat", req, resp, opts...)
	return resp, err
}
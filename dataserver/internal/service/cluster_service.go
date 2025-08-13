package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
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
	
	// Leader发现和监听
	currentLeader  string
	leaderWatcher  clientv3.WatchChan
	leaderStopChan chan struct{}
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
	
	// 发现当前Leader
	leader, err := discoverLeader(etcdClient)
	if err != nil {
		etcdClient.Close()
		return nil, fmt.Errorf("failed to discover leader: %w", err)
	}
	
	// 创建metaserver连接到Leader
	ctx, cancel := context.WithTimeout(context.Background(), 
		time.Duration(config.MetaServer.ConnectionTimeout)*time.Second)
	defer cancel()
	
	metaConn, err := grpc.DialContext(ctx, leader,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		etcdClient.Close()
		return nil, fmt.Errorf("failed to connect to leader metaserver %s: %w", leader, err)
	}
	
	service := &EtcdClusterService{
		config:         config,
		etcdClient:     etcdClient,
		metaClient:     metaConn,
		storageService: storageService,
		lease:          clientv3.NewLease(etcdClient),
		stopChan:       make(chan struct{}),
		leaderStopChan: make(chan struct{}),
		currentLeader:  leader,
	}
	
	// 启动Leader监听
	service.startLeaderWatcher()
	
	return service, nil
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

// startLeaderWatcher 启动Leader变化监听
func (s *EtcdClusterService) startLeaderWatcher() {
	// 监听Leader变化 - 监听具体的leader key
	s.leaderWatcher = s.etcdClient.Watch(context.Background(), "/dfs/metaserver/leader/current")
	
	go func() {
		log.Println("Leader watcher started, monitoring /dfs/metaserver/leader/current")
		
		for {
			select {
			case watchResp := <-s.leaderWatcher:
				for _, event := range watchResp.Events {
					log.Printf("Leader change detected: %s on key %s, value: %s", 
						event.Type, string(event.Kv.Key), string(event.Kv.Value))
					
					// 当Leader发生变化时，重新连接
					if err := s.handleLeaderChange(); err != nil {
						log.Printf("Failed to handle leader change: %v", err)
					} else {
						log.Printf("Successfully handled leader change")
					}
				}
				
			case <-s.leaderStopChan:
				log.Println("Leader watcher stopping")
				return
			}
		}
	}()
}

// handleLeaderChange 处理Leader变化
func (s *EtcdClusterService) handleLeaderChange() error {
	log.Println("Handling leader change...")
	
	// 发现新的Leader
	newLeader, err := discoverLeader(s.etcdClient)
	if err != nil {
		return fmt.Errorf("failed to discover new leader: %w", err)
	}
	
	if newLeader == s.currentLeader {
		log.Printf("Leader unchanged: %s", s.currentLeader)
		return nil // 没有变化
	}
	
	log.Printf("Leader changed from %s to %s, reconnecting...", s.currentLeader, newLeader)
	
	// 关闭旧连接
	if s.metaClient != nil {
		s.metaClient.Close()
	}
	
	// 创建新连接
	ctx, cancel := context.WithTimeout(context.Background(), 
		time.Duration(s.config.MetaServer.ConnectionTimeout)*time.Second)
	defer cancel()
	
	newConn, err := grpc.DialContext(ctx, newLeader,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to new leader %s: %w", newLeader, err)
	}
	
	s.metaClient = newConn
	s.currentLeader = newLeader
	
	log.Printf("Successfully reconnected to new leader: %s", newLeader)
	return nil
}

// Stop 停止集群服务
func (s *EtcdClusterService) Stop() error {
	if !s.isRunning {
		return nil
	}
	
	// 停止Leader监听
	close(s.leaderStopChan)
	
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
		TotalCapacity:  stat.TotalCapacity,
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
		log.Printf("Heartbeat failed, attempting to reconnect to leader: %v", err)
		// 尝试重连到新的Leader
		if reconnectErr := s.reconnectToLeader(); reconnectErr != nil {
			return fmt.Errorf("failed to reconnect to leader: %w", reconnectErr)
		}
		
		// 重新创建客户端并重试
		client = NewMetaServerServiceClient(s.metaClient)
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		resp, err = client.Heartbeat(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to send heartbeat after reconnect: %w", err)
		}
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

// discoverLeader 从etcd发现当前Leader
func discoverLeader(etcdClient *clientv3.Client) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// 查询etcd中的Leader信息
	resp, err := etcdClient.Get(ctx, "/dfs/metaserver/leader/current")
	if err != nil {
		return "", fmt.Errorf("failed to query leader from etcd: %w", err)
	}
	
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("no leader found in etcd")
	}
	
	// Leader的地址从key中提取节点ID，然后查询节点信息
	leaderNodeID := string(resp.Kvs[0].Value)
	log.Printf("Found leader node ID: %s", leaderNodeID)
	
	// 查询节点信息
	nodeResp, err := etcdClient.Get(ctx, fmt.Sprintf("/dfs/metaserver/nodes/%s", leaderNodeID))
	if err != nil {
		return "", fmt.Errorf("failed to get leader node info: %w", err)
	}
	
	if len(nodeResp.Kvs) == 0 {
		return "", fmt.Errorf("leader node info not found")
	}
	
	// 解析节点地址 - 应该是JSON格式
	nodeInfo := string(nodeResp.Kvs[0].Value)
	log.Printf("Leader node info: %s", nodeInfo)
	
	// 尝试解析JSON格式
	var node struct {
		Addr string `json:"addr"`
	}
	
	if err := json.Unmarshal([]byte(nodeInfo), &node); err == nil && node.Addr != "" {
		log.Printf("Discovered leader address: %s", node.Addr)
		return node.Addr, nil
	}
	
	// 如果JSON解析失败，尝试简单的字符串解析
	if strings.Contains(nodeInfo, "\"addr\":\"") {
		// JSON格式，提取addr字段
		parts := strings.Split(nodeInfo, "\"addr\":\"")
		if len(parts) > 1 {
			addr := strings.Split(parts[1], "\"")[0]
			log.Printf("Parsed leader address: %s", addr)
			return addr, nil
		}
	}
	
	// 如果还是失败，尝试直接使用节点信息作为地址
	log.Printf("Using node info as address: %s", nodeInfo)
	return nodeInfo, nil
}

// reconnectToLeader 重连到新的Leader
func (s *EtcdClusterService) reconnectToLeader() error {
	// 发现新的Leader
	newLeader, err := discoverLeader(s.etcdClient)
	if err != nil {
		return fmt.Errorf("failed to discover new leader: %w", err)
	}
	
	if newLeader == s.currentLeader {
		return nil // 没有变化
	}
	
	log.Printf("Leader changed from %s to %s, reconnecting...", s.currentLeader, newLeader)
	
	// 关闭旧连接
	if s.metaClient != nil {
		s.metaClient.Close()
	}
	
	// 创建新连接
	ctx, cancel := context.WithTimeout(context.Background(), 
		time.Duration(s.config.MetaServer.ConnectionTimeout)*time.Second)
	defer cancel()
	
	newConn, err := grpc.DialContext(ctx, newLeader,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to new leader %s: %w", newLeader, err)
	}
	
	s.metaClient = newConn
	s.currentLeader = newLeader
	
	log.Printf("Successfully reconnected to new leader: %s", newLeader)
	return nil
}

// 使用生成的MetaServer客户端
func NewMetaServerServiceClient(conn *grpc.ClientConn) pb.MetaServerServiceClient {
	return pb.NewMetaServerServiceClient(conn)
}
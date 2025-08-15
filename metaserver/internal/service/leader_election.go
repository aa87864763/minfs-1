package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"metaserver/internal/model"
	"metaserver/pb"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// LeaderElection 实现基于etcd Campaign的分布式leader选举
type LeaderElection struct {
	etcdClient   *clientv3.Client
	nodeID       string
	nodeAddr     string
	config       *model.Config
	
	// Campaign选举相关
	session      *concurrency.Session
	election     *concurrency.Election
	
	// 状态相关
	isLeader      bool
	currentLeader *MetaServerNode
	followers     []*MetaServerNode
	
	// 控制相关
	mutex         sync.RWMutex
	stopChan      chan bool
	campaignCtx   context.Context
	campaignCancel context.CancelFunc
	
	// 回调函数
	onLeaderChange func(isLeader bool)
	
	// WAL服务引用
	walService *WALService
}

// MetaServerNode 表示一个MetaServer节点
type MetaServerNode struct {
	NodeID   string `json:"node_id"`
	Host     string `json:"host"`
	Port     int32  `json:"port"`
	Addr     string `json:"addr"`
	JoinTime time.Time `json:"join_time"`
}

// NewLeaderElection 创建新的选举服务
func NewLeaderElection(config *model.Config, nodeID, nodeAddr string) (*LeaderElection, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Etcd.Endpoints,
		DialTimeout: config.Etcd.Timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %v", err)
	}

	// 创建session
	session, err := concurrency.NewSession(client, concurrency.WithTTL(30))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create etcd session: %v", err)
	}

	// 创建election对象
	election := concurrency.NewElection(session, "/minfs/metaserver/election")

	le := &LeaderElection{
		etcdClient: client,
		session:    session,
		election:   election,
		nodeID:     nodeID,
		nodeAddr:   nodeAddr,
		config:     config,
		stopChan:   make(chan bool),
		followers:  make([]*MetaServerNode, 0),
	}

	return le, nil
}

// SetWALService 设置WAL服务引用
func (le *LeaderElection) SetWALService(walService *WALService) {
	le.mutex.Lock()
	defer le.mutex.Unlock()
	le.walService = walService
}

// Start 启动选举服务
func (le *LeaderElection) Start() error {
	// 注册节点信息
	if err := le.registerNode(); err != nil {
		return fmt.Errorf("failed to register node: %v", err)
	}

	// 创建campaign context
	le.campaignCtx, le.campaignCancel = context.WithCancel(context.Background())

	// 启动选举goroutine
	go le.campaignLoop()
	
	// 启动监听goroutine
	go le.watchLeader()
	go le.watchNodes()

	log.Printf("LeaderElection started for node %s at %s", le.nodeID, le.nodeAddr)
	return nil
}

// campaignLoop 选举循环 - 使用etcd Campaign API
func (le *LeaderElection) campaignLoop() {
	for {
		select {
		case <-le.stopChan:
			return
		default:
			log.Printf("Node %s starting campaign for leadership", le.nodeID)
			
			// 创建节点信息作为candidate value
			nodeInfo := fmt.Sprintf("%s:%s", le.nodeID, le.nodeAddr)
			
			// 开始campaign - 这会阻塞直到成为leader或context被取消
			err := le.election.Campaign(le.campaignCtx, nodeInfo)
			if err != nil {
				if err == context.Canceled {
					log.Printf("Node %s campaign canceled", le.nodeID)
					return
				}
				log.Printf("Node %s campaign failed: %v, retrying in 5s", le.nodeID, err)
				time.Sleep(5 * time.Second)
				continue
			}
			
			le.mutex.Lock()
			le.isLeader = true
			le.mutex.Unlock()
			
			log.Printf("Node %s successfully became leader", le.nodeID)
			if le.onLeaderChange != nil {
				le.onLeaderChange(true)
			}
			
			// 更新followers列表并通知WAL服务
			le.updateFollowers()
			
			// 保持leader身份直到session过期或主动退出
			le.maintainLeadership()
			
			// 失去leadership
			le.mutex.Lock()
			le.isLeader = false
			le.mutex.Unlock()
			
			log.Printf("Node %s lost leadership", le.nodeID)
			if le.onLeaderChange != nil {
				le.onLeaderChange(false)
			}
		}
	}
}


// maintainLeadership 维持leader身份
func (le *LeaderElection) maintainLeadership() {
	// 等待session过期或收到停止信号
	select {
	case <-le.session.Done():
		// Session已过期，自动失去leadership
		log.Printf("Node %s session expired, losing leadership", le.nodeID)
		return
		
	case <-le.stopChan:
		// 收到停止信号，主动resign
		log.Printf("Node %s received stop signal, resigning leadership", le.nodeID)
		le.election.Resign(context.Background())
		return
		
	case <-le.campaignCtx.Done():
		// Campaign context被取消
		log.Printf("Node %s campaign context canceled, losing leadership", le.nodeID)
		return
	}
}

// watchLeader 监听leader变化
func (le *LeaderElection) watchLeader() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// 使用election的Observe方法监听leader变化
	observeChan := le.election.Observe(ctx)
	
	for {
		select {
		case resp, ok := <-observeChan:
			if !ok {
				log.Printf("Leader observe channel closed")
				return
			}
			
			if len(resp.Kvs) > 0 {
				// 解析leader信息
				leaderInfo := string(resp.Kvs[0].Value)
				le.updateCurrentLeaderFromInfo(leaderInfo)
			}
			
		case <-le.stopChan:
			cancel()
			return
		}
	}
}

// watchNodes 监听节点变化
func (le *LeaderElection) watchNodes() {
	watchChan := le.etcdClient.Watch(context.Background(), "/minfs/metaserver/nodes/", clientv3.WithPrefix())
	
	for {
		select {
		case watchResp := <-watchChan:
			for range watchResp.Events {
				// 节点发生变化，更新followers列表
				le.updateFollowers()
			}
		case <-le.stopChan:
			return
		}
	}
}

// registerNode 注册节点信息到etcd
func (le *LeaderElection) registerNode() error {
	host, port := le.parseAddr(le.nodeAddr)
	
	node := &MetaServerNode{
		NodeID:   le.nodeID,
		Host:     host,
		Port:     port,
		Addr:     le.nodeAddr,
		JoinTime: time.Now(),
	}

	nodeData, err := json.Marshal(node)
	if err != nil {
		return err
	}

	// 使用lease确保节点信息会过期
	lease, err := le.etcdClient.Grant(context.Background(), 60) // 60秒TTL
	if err != nil {
		return err
	}

	key := fmt.Sprintf("/minfs/metaserver/nodes/%s", le.nodeID)
	_, err = le.etcdClient.Put(context.Background(), key, string(nodeData), clientv3.WithLease(lease.ID))
	if err != nil {
		return err
	}

	// 定期续租
	ch, kaerr := le.etcdClient.KeepAlive(context.Background(), lease.ID)
	if kaerr != nil {
		return kaerr
	}

	// 处理续租响应
	go func() {
		for ka := range ch {
			_ = ka // 处理续租响应
		}
	}()

	return nil
}

// updateCurrentLeaderFromInfo 从leader信息字符串更新当前leader
func (le *LeaderElection) updateCurrentLeaderFromInfo(leaderInfo string) {
	// 解析leader信息格式: "nodeID:nodeAddr"
	parts := strings.Split(leaderInfo, ":")
	if len(parts) < 2 {
		log.Printf("Invalid leader info format: %s", leaderInfo)
		return
	}
	
	nodeID := parts[0]
	nodeAddr := strings.Join(parts[1:], ":")
	
	// 获取leader节点详细信息
	key := fmt.Sprintf("/minfs/metaserver/nodes/%s", nodeID)
	resp, err := le.etcdClient.Get(context.Background(), key)
	if err != nil {
		log.Printf("Failed to get leader node info: %v", err)
		return
	}

	if len(resp.Kvs) > 0 {
		var leaderNode MetaServerNode
		if err := json.Unmarshal(resp.Kvs[0].Value, &leaderNode); err != nil {
			log.Printf("Failed to unmarshal leader node info: %v", err)
			return
		}
		
		le.mutex.Lock()
		le.currentLeader = &leaderNode
		le.mutex.Unlock()
		
		log.Printf("Leader updated: %s at %s", nodeID, nodeAddr)
	}
}

// updateCurrentLeader 更新当前leader信息
func (le *LeaderElection) updateCurrentLeader(leaderNodeID string) {
	// 获取leader节点详细信息
	key := fmt.Sprintf("/minfs/metaserver/nodes/%s", leaderNodeID)
	resp, err := le.etcdClient.Get(context.Background(), key)
	if err != nil {
		log.Printf("Failed to get leader node info: %v", err)
		return
	}

	if len(resp.Kvs) == 0 {
		return
	}

	var leaderNode MetaServerNode
	if err := json.Unmarshal(resp.Kvs[0].Value, &leaderNode); err != nil {
		log.Printf("Failed to unmarshal leader node info: %v", err)
		return
	}

	le.mutex.Lock()
	le.currentLeader = &leaderNode
	le.mutex.Unlock()
}

// updateFollowers 更新followers列表
func (le *LeaderElection) updateFollowers() {
	resp, err := le.etcdClient.Get(context.Background(), "/minfs/metaserver/nodes/", clientv3.WithPrefix())
	if err != nil {
		log.Printf("Failed to get all nodes: %v", err)
		return
	}

	var allNodes []*MetaServerNode

	// 解析所有节点
	for _, kv := range resp.Kvs {
		var node MetaServerNode
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			continue
		}
		allNodes = append(allNodes, &node)
	}

	// 获取当前leader通过election API
	var leader *MetaServerNode
	leaderResp, err := le.election.Leader(context.Background())
	if err == nil && len(leaderResp.Kvs) > 0 {
		// 解析leader信息
		leaderInfo := string(leaderResp.Kvs[0].Value)
		parts := strings.Split(leaderInfo, ":")
		if len(parts) >= 2 {
			leaderNodeID := parts[0]
			for _, node := range allNodes {
				if node.NodeID == leaderNodeID {
					leader = node
					break
				}
			}
		}
	}

	// 构建followers列表
	var followers []*MetaServerNode
	for _, node := range allNodes {
		if leader == nil || node.NodeID != leader.NodeID {
			followers = append(followers, node)
		}
	}

	le.mutex.Lock()
	le.currentLeader = leader
	le.followers = followers
	walService := le.walService
	le.mutex.Unlock()
	
	// 如果当前节点是leader且有WAL服务，更新WAL服务的followers
	if le.isLeader && walService != nil {
		walService.UpdateFollowers(followers)
	}
}

// GetCurrentLeader 获取当前leader信息
func (le *LeaderElection) GetCurrentLeader() *pb.MetaServerMsg {
	le.mutex.RLock()
	defer le.mutex.RUnlock()

	if le.currentLeader == nil {
		// 如果没有leader，返回默认值
		return &pb.MetaServerMsg{
			Host: "localhost",
			Port: 9090,
		}
	}

	return &pb.MetaServerMsg{
		Host: le.currentLeader.Host,
		Port: le.currentLeader.Port,
	}
}

// GetFollowers 获取followers列表
func (le *LeaderElection) GetFollowers() []*pb.MetaServerMsg {
	le.mutex.RLock()
	defer le.mutex.RUnlock()

	var followers []*pb.MetaServerMsg
	for _, follower := range le.followers {
		followers = append(followers, &pb.MetaServerMsg{
			Host: follower.Host,
			Port: follower.Port,
		})
	}

	return followers
}

// IsLeader 检查当前节点是否为leader
func (le *LeaderElection) IsLeader() bool {
	le.mutex.RLock()
	defer le.mutex.RUnlock()
	return le.isLeader
}

// SetLeaderChangeCallback 设置leader变化回调
func (le *LeaderElection) SetLeaderChangeCallback(callback func(bool)) {
	le.onLeaderChange = callback
}

// parseAddr 解析地址
func (le *LeaderElection) parseAddr(addr string) (string, int32) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return "localhost", 9090
	}
	
	host := parts[0]
	port := int32(9090) // 默认端口
	
	if p, err := strconv.Atoi(parts[1]); err == nil {
		port = int32(p)
	}
	
	return host, port
}

// Stop 停止选举服务
func (le *LeaderElection) Stop() {
	close(le.stopChan)
	
	// 取消campaign context
	if le.campaignCancel != nil {
		le.campaignCancel()
	}
	
	// 如果当前是leader，主动resign
	if le.isLeader && le.election != nil {
		le.election.Resign(context.Background())
	}
	
	// 关闭session
	if le.session != nil {
		le.session.Close()
	}
	
	// 关闭etcd客户端
	if le.etcdClient != nil {
		le.etcdClient.Close()
	}
	
	log.Printf("LeaderElection stopped for node %s", le.nodeID)
}
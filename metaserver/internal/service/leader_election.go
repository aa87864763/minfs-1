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
)

// LeaderElection 实现基于etcd的分布式leader选举
type LeaderElection struct {
	etcdClient   *clientv3.Client
	nodeID       string
	nodeAddr     string
	config       *model.Config
	
	// 选举相关
	isLeader     bool
	currentLeader *MetaServerNode
	followers    []*MetaServerNode
	
	// 新的分布式锁相关字段
	leaderLease        clientv3.LeaseID
	leaseKeepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	
	// 状态锁
	mutex        sync.RWMutex
	stopChan     chan bool
	
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

	le := &LeaderElection{
		etcdClient: client,
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

	// 启动选举goroutine
	go le.campaignLoop()
	
	// 启动监听goroutine
	go le.watchLeader()
	go le.watchNodes()

	log.Printf("LeaderElection started for node %s at %s", le.nodeID, le.nodeAddr)
	return nil
}

// campaignLoop 选举循环 - 使用简单的分布式锁机制
func (le *LeaderElection) campaignLoop() {
	for {
		select {
		case <-le.stopChan:
			return
		default:
			// 尝试获取leader锁
			log.Printf("Node %s attempting to acquire leader lock", le.nodeID)
			
			if le.tryAcquireLeaderLock() {
				// 成功获得leader锁
				le.mutex.Lock()
				le.isLeader = true
				le.mutex.Unlock()
				
				log.Printf("Node %s successfully became leader", le.nodeID)
				if le.onLeaderChange != nil {
					le.onLeaderChange(true)
				}
				
				// 更新followers列表并通知WAL服务
				le.updateFollowers()
				
				// 保持leader身份并定期续约
				le.maintainLeadership()
				
				// 失去leadership
				le.mutex.Lock()
				le.isLeader = false
				le.mutex.Unlock()
				
				log.Printf("Node %s lost leadership", le.nodeID)
				if le.onLeaderChange != nil {
					le.onLeaderChange(false)
				}
			} else {
				// 未能获得leader锁，等待一段时间后重试
				log.Printf("Node %s failed to acquire leader lock, retrying in 5s", le.nodeID)
				time.Sleep(5 * time.Second)
			}
		}
	}
}

// tryAcquireLeaderLock 尝试获取leader锁
func (le *LeaderElection) tryAcquireLeaderLock() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// 使用etcd的原子操作来实现分布式锁
	leaderKey := "/dfs/metaserver/leader/current"
	
	// 创建租约
	lease, err := le.etcdClient.Grant(ctx, 30) // 30秒TTL
	if err != nil {
		log.Printf("Failed to create lease for leader lock: %v", err)
		return false
	}
	
	// 尝试以原子方式创建leader key（仅当key不存在时）
	resp, err := le.etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(leaderKey), "=", 0)).
		Then(clientv3.OpPut(leaderKey, le.nodeID, clientv3.WithLease(lease.ID))).
		Commit()
	
	if err != nil {
		log.Printf("Failed to acquire leader lock: %v", err)
		return false
	}
	
	if !resp.Succeeded {
		// 锁已被其他节点持有
		return false
	}
	
	// 成功获得锁，保存lease ID用于后续续约
	le.leaderLease = lease.ID
	
	// 启动续约
	ch, kaerr := le.etcdClient.KeepAlive(context.Background(), lease.ID)
	if kaerr != nil {
		log.Printf("Failed to start lease keep-alive: %v", kaerr)
		return false
	}
	
	le.leaseKeepAliveChan = ch
	return true
}

// maintainLeadership 维持leader身份
func (le *LeaderElection) maintainLeadership() {
	defer func() {
		// 清理lease keep-alive
		if le.leaseKeepAliveChan != nil {
			// 取消lease
			le.etcdClient.Revoke(context.Background(), le.leaderLease)
		}
	}()
	
	// 监听续约响应和停止信号
	for {
		select {
		case ka := <-le.leaseKeepAliveChan:
			if ka == nil {
				// Keep-alive channel关闭，说明lease失效
				log.Printf("Node %s lease expired, losing leadership", le.nodeID)
				return
			}
			// 续约成功，继续保持leadership
			
		case <-le.stopChan:
			// 收到停止信号
			log.Printf("Node %s received stop signal, releasing leadership", le.nodeID)
			return
			
		case <-time.After(35 * time.Second):
			// 超时检查，确保lease还在有效期内
			log.Printf("Node %s leadership timeout check", le.nodeID)
			// 可以在这里添加额外的健康检查
		}
	}
}

// watchLeader 监听leader变化
func (le *LeaderElection) watchLeader() {
	watchChan := le.etcdClient.Watch(context.Background(), "/dfs/metaserver/leader/", clientv3.WithPrefix())
	
	for {
		select {
		case watchResp := <-watchChan:
			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypePut {
					// leader发生变化
					leaderNodeID := string(event.Kv.Value)
					le.updateCurrentLeader(leaderNodeID)
				}
			}
		case <-le.stopChan:
			return
		}
	}
}

// watchNodes 监听节点变化
func (le *LeaderElection) watchNodes() {
	watchChan := le.etcdClient.Watch(context.Background(), "/dfs/metaserver/nodes/", clientv3.WithPrefix())
	
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

	key := fmt.Sprintf("/dfs/metaserver/nodes/%s", le.nodeID)
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

// updateCurrentLeader 更新当前leader信息
func (le *LeaderElection) updateCurrentLeader(leaderNodeID string) {
	// 获取leader节点详细信息
	key := fmt.Sprintf("/dfs/metaserver/nodes/%s", leaderNodeID)
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
	resp, err := le.etcdClient.Get(context.Background(), "/dfs/metaserver/nodes/", clientv3.WithPrefix())
	if err != nil {
		log.Printf("Failed to get all nodes: %v", err)
		return
	}

	var allNodes []*MetaServerNode
	var leader *MetaServerNode

	// 解析所有节点
	for _, kv := range resp.Kvs {
		var node MetaServerNode
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			continue
		}
		allNodes = append(allNodes, &node)
	}

	// 获取当前leader
	leaderResp, err := le.etcdClient.Get(context.Background(), "/dfs/metaserver/leader/current")
	if err == nil && len(leaderResp.Kvs) > 0 {
		leaderNodeID := string(leaderResp.Kvs[0].Value)
		for _, node := range allNodes {
			if node.NodeID == leaderNodeID {
				leader = node
				break
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
	
	// 如果当前是leader，释放leader锁
	if le.isLeader && le.leaderLease != 0 {
		le.etcdClient.Revoke(context.Background(), le.leaderLease)
	}
	
	if le.etcdClient != nil {
		le.etcdClient.Close()
	}
	
	log.Printf("LeaderElection stopped for node %s", le.nodeID)
}
package service

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"metaServer/internal/model"
	"metaServer/pb"
)

// BlockReplicationCallback 块复制完成回调函数类型
type BlockReplicationCallback func(blockID uint64, targetAddr string, success bool)

type ClusterService struct {
	config      *model.Config
	dataServers map[string]*model.DataServerInfo // Key: DataServer ID
	mutex       sync.RWMutex

	// 用于存储需要下发的命令
	pendingCommands map[string][]*model.Command // Key: DataServer ID
	commandMutex    sync.RWMutex

	// 用于健康检查
	healthCheckTicker *time.Ticker
	stopChan          chan bool

	// etcd 服务发现
	etcdService *EtcdService

	// Leader Election 服务
	leaderElection *LeaderElection

	// 块复制完成回调
	replicationCallback BlockReplicationCallback
}

func NewClusterService(config *model.Config) *ClusterService {
	cs := &ClusterService{
		config:          config,
		dataServers:     make(map[string]*model.DataServerInfo),
		pendingCommands: make(map[string][]*model.Command),
		stopChan:        make(chan bool),
	}

	// 初始化 etcd 服务
	etcdService, err := NewEtcdService(config)
	if err != nil {
		log.Printf("Warning: Failed to initialize etcd service: %v", err)
		log.Printf("Falling back to heartbeat-only mode")
	} else {
		cs.etcdService = etcdService
		// 开始监听 DataServer 变化
		err = etcdService.StartWatching(cs.onDataServerChange)
		if err != nil {
			log.Printf("Warning: Failed to start etcd watching: %v", err)
		}
	}

	// 启动健康检查
	cs.startHealthCheck()

	return cs
}

// SetLeaderElection 设置Leader Election服务
func (cs *ClusterService) SetLeaderElection(le *LeaderElection) {
	cs.leaderElection = le
}

// SetReplicationCallback 设置块复制完成回调
func (cs *ClusterService) SetReplicationCallback(callback BlockReplicationCallback) {
	cs.replicationCallback = callback
}

// IsLeader 检查当前节点是否为leader
func (cs *ClusterService) IsLeader() bool {
	if cs.leaderElection == nil {
		return false
	}
	return cs.leaderElection.IsLeader()
}

// startHealthCheck 启动健康检查goroutine
func (cs *ClusterService) startHealthCheck() {
	cs.healthCheckTicker = time.NewTicker(cs.config.Cluster.HeartbeatTimeout / 2)

	go func() {
		for {
			select {
			case <-cs.healthCheckTicker.C:
				cs.checkDataServerHealth()
			case <-cs.stopChan:
				return
			}
		}
	}()
}

// checkDataServerHealth 检查 DataServer 健康状态
func (cs *ClusterService) checkDataServerHealth() {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	now := time.Now()

	for id, ds := range cs.dataServers {
		_, _, _, lastHeartbeat, isHealthy := ds.GetStatus()

		// 如果超过心跳超时时间，标记为不健康
		if isHealthy && now.Sub(lastHeartbeat) > cs.config.Cluster.HeartbeatTimeout {
			ds.MarkUnhealthy()
			log.Printf("DataServer %s marked as unhealthy (last heartbeat: %v)", id, lastHeartbeat)
		}

		// 检查是否应该标记为永久宕机
		if !isHealthy && !ds.IsPermanentlyDownStatus() {
			unhealthyDuration := ds.GetUnhealthyDuration()
			if unhealthyDuration > cs.config.Cluster.PermanentDownThreshold {
				ds.MarkPermanentlyDown()
				log.Printf("DataServer %s marked as permanently down (unhealthy for %v)", id, unhealthyDuration)

				// 触发副本重分布
				cs.triggerReplicaRedistribution(id, ds.Addr)
			}
		}
	}
}

// ProcessHeartbeat 处理来自 DataServer 的心跳
func (cs *ClusterService) ProcessHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	cs.mutex.Lock()

	// 更新或创建 DataServer 信息
	ds, exists := cs.dataServers[req.DataserverId]
	if !exists {
		ds = &model.DataServerInfo{
			ID:             req.DataserverId,
			Addr:           req.DataserverAddr,
			ReportedBlocks: make(map[uint64]bool),
		}
		cs.dataServers[req.DataserverId] = ds
		log.Printf("New DataServer registered: %s at %s", req.DataserverId, req.DataserverAddr)
	}

	// 更新状态和块报告
	wasUnhealthy := !ds.IsHealthy
	ds.UpdateStatus(req.BlockCount, req.FreeSpace, req.TotalCapacity)

	// 如果节点从不健康状态恢复，重置永久宕机标志
	if wasUnhealthy {
		ds.RecoverToHealthy()
		log.Printf("DataServer %s recovered to healthy state", req.DataserverId)
	}

	newBlocks := ds.UpdateReportedBlocks(req.BlockIdsReport)
	cs.mutex.Unlock()

	// 如果有回调函数且有新增的块，触发回调
	if cs.replicationCallback != nil && len(newBlocks) > 0 {
		for _, blockID := range newBlocks {
			cs.replicationCallback(blockID, req.DataserverAddr, true)
		}
	}

	// 获取待下发的命令
	cs.commandMutex.Lock()
	commands := cs.pendingCommands[req.DataserverId]
	// 清空已下发的命令
	delete(cs.pendingCommands, req.DataserverId)
	cs.commandMutex.Unlock()

	// 转换为 protobuf 格式
	var pbCommands []*pb.Command
	for _, cmd := range commands {
		pbCmd := &pb.Command{
			BlockId: cmd.BlockID,
			Targets: cmd.Targets,
		}

		switch cmd.Action {
		case "DELETE_BLOCK":
			pbCmd.Action = pb.Command_DELETE_BLOCK
		case "COPY_BLOCK":
			pbCmd.Action = pb.Command_COPY_BLOCK
		}

		pbCommands = append(pbCommands, pbCmd)
	}

	log.Printf("Heartbeat from %s: %d blocks, %d MB free, %d commands sent",
		req.DataserverId, req.BlockCount, req.FreeSpace/1024/1024, len(pbCommands))

	return &pb.HeartbeatResponse{
		Commands: pbCommands,
	}, nil
}

// GetHealthyDataServers 获取所有健康的 DataServer
func (cs *ClusterService) GetHealthyDataServers() []*model.DataServerInfo {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	var healthyServers []*model.DataServerInfo

	for _, ds := range cs.dataServers {
		_, _, _, _, isHealthy := ds.GetStatus()
		if isHealthy {
			healthyServers = append(healthyServers, ds)
		}
	}

	return healthyServers
}

// GetAllDataServers 获取所有 DataServer 信息
func (cs *ClusterService) GetAllDataServers() []*model.DataServerInfo {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	var servers []*model.DataServerInfo
	for _, ds := range cs.dataServers {
		servers = append(servers, ds)
	}

	return servers
}

// SelectDataServersForThreeReplica 为三副本存储选择服务器（全局负载均衡）
func (cs *ClusterService) SelectDataServersForThreeReplica(totalPhysicalBlocks int) ([]string, error) {
	healthyServers := cs.GetHealthyDataServers()

	if len(healthyServers) < 3 {
		return nil, fmt.Errorf("need at least 3 healthy DataServers for three-replica, have %d", len(healthyServers))
	}

	serverCount := len(healthyServers)

	// 为了负载均衡，我们采用轮询分配策略
	var assignments []string
	serverIndex := 0

	for i := 0; i < totalPhysicalBlocks; i++ {
		// 选择当前服务器
		selectedServer := healthyServers[serverIndex%serverCount]
		assignments = append(assignments, selectedServer.Addr)

		// 移动到下一个服务器
		serverIndex++
	}

	log.Printf("Three-replica distribution: %d physical blocks distributed across %d servers",
		totalPhysicalBlocks, serverCount)

	// 统计每个服务器分配到的块数量
	serverBlockCount := make(map[string]int)
	for _, addr := range assignments {
		serverBlockCount[addr]++
	}

	for addr, count := range serverBlockCount {
		log.Printf("  Server %s: %d blocks", addr, count)
	}

	return assignments, nil
}

// SendCommand 向指定的 DataServer 发送命令
func (cs *ClusterService) SendCommand(dataServerID string, command *model.Command) {
	cs.commandMutex.Lock()
	defer cs.commandMutex.Unlock()

	cs.pendingCommands[dataServerID] = append(cs.pendingCommands[dataServerID], command)

	log.Printf("Command queued for %s: %s block %d", dataServerID, command.Action, command.BlockID)
}

// SendCommandToMultiple 向多个 DataServer 发送相同的命令
func (cs *ClusterService) SendCommandToMultiple(dataServerIDs []string, command *model.Command) {
	cs.commandMutex.Lock()
	defer cs.commandMutex.Unlock()

	for _, dsID := range dataServerIDs {
		cs.pendingCommands[dsID] = append(cs.pendingCommands[dsID], command)
	}

	log.Printf("Command queued for %d servers: %s block %d",
		len(dataServerIDs), command.Action, command.BlockID)
}

// GetDataServerByID 根据 ID 获取 DataServer
func (cs *ClusterService) GetDataServerByID(id string) *model.DataServerInfo {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	return cs.dataServers[id]
}

// GetDataServerByAddr 根据地址获取 DataServer
func (cs *ClusterService) GetDataServerByAddr(addr string) *model.DataServerInfo {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	for _, ds := range cs.dataServers {
		if ds.Addr == addr {
			return ds
		}
	}

	return nil
}

// GetClusterStats 获取集群统计信息
func (cs *ClusterService) GetClusterStats() (totalServers, healthyServers int, totalBlocks, totalFreeSpace uint64) {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	totalServers = len(cs.dataServers)

	for _, ds := range cs.dataServers {
		blockCount, freeSpace, _, _, isHealthy := ds.GetStatus()

		if isHealthy {
			healthyServers++
		}

		totalBlocks += blockCount
		totalFreeSpace += freeSpace
	}

	return
}

// GetClusterInfo 获取集群详细信息（用于gRPC接口）
func (cs *ClusterService) GetClusterInfo() *pb.ClusterInfo {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	var dataServers []*pb.DataServerMsg

	for _, ds := range cs.dataServers {
		blockCount, freeSpace, totalCapacity, _, _ := ds.GetStatus()

		// 解析地址
		host, port := cs.parseAddr(ds.Addr)

		// 使用真实的容量数据，转换为MB
		totalCapacityMB := int32(totalCapacity / 1024 / 1024)
		freeSpaceMB := int32(freeSpace / 1024 / 1024)
		useCapacityMB := totalCapacityMB - freeSpaceMB

		dataServerMsg := &pb.DataServerMsg{
			Host:        host,
			Port:        port,
			FileTotal:   int32(blockCount),
			Capacity:    totalCapacityMB,
			UseCapacity: useCapacityMB,
		}

		dataServers = append(dataServers, dataServerMsg)
	}

	// 获取真实的主从信息
	var masterMetaServer *pb.MetaServerMsg
	var slaveMetaServers []*pb.MetaServerMsg

	if cs.leaderElection != nil {
		masterMetaServer = cs.leaderElection.GetCurrentLeader()
		slaveMetaServers = cs.leaderElection.GetFollowers()
	} else {
		// 如果没有选举服务，返回默认值
		masterMetaServer = &pb.MetaServerMsg{
			Host: "localhost",
			Port: 8080,
		}
		slaveMetaServers = []*pb.MetaServerMsg{}
	}

	// 构建集群信息
	clusterInfo := &pb.ClusterInfo{
		MasterMetaServer: masterMetaServer,
		SlaveMetaServer:  slaveMetaServers,
		DataServer:       dataServers,
	}

	return clusterInfo
}

// parseAddr 解析地址字符串 "host:port" 为单独的 host 和 port
func (cs *ClusterService) parseAddr(addr string) (string, int32) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return "localhost", 8090
	}

	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return parts[0], 8090
	}

	return parts[0], int32(port)
}

// onDataServerChange 处理 etcd 中 DataServer 的变化事件
func (cs *ClusterService) onDataServerChange(id string, registration *DataServerRegistration, isOnline bool) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	if isOnline {
		// DataServer 上线
		if ds, exists := cs.dataServers[id]; exists {
			// 更新地址信息并恢复健康状态
			ds.Addr = registration.Addr
			ds.RecoverToHealthy() // 确保恢复到健康状态
			log.Printf("DataServer address updated and recovered: %s -> %s", id, registration.Addr)
		} else {
			// 新的 DataServer
			ds = &model.DataServerInfo{
				ID:             registration.ID,
				Addr:           registration.Addr,
				ReportedBlocks: make(map[uint64]bool),
			}
			ds.RecoverToHealthy() // 明确标记为健康
			cs.dataServers[id] = ds
			log.Printf("DataServer discovered from etcd: %s at %s", id, registration.Addr)
		}
	} else {
		// DataServer 下线 - 立即从集群中移除
		if ds, exists := cs.dataServers[id]; exists {
			log.Printf("DataServer removed from cluster immediately due to etcd deletion: %s (was at %s)", id, ds.Addr)

			// 立即从集群中移除，这样GetClusterInfo就能实时反映变化
			delete(cs.dataServers, id)

			// 触发副本重分布
			cs.triggerReplicaRedistribution(id, ds.Addr)
		}
	}
}

// Stop 停止集群服务
func (cs *ClusterService) Stop() {
	if cs.healthCheckTicker != nil {
		cs.healthCheckTicker.Stop()
	}

	if cs.etcdService != nil {
		cs.etcdService.Stop()
	}

	select {
	case cs.stopChan <- true:
	default:
	}

	log.Println("ClusterService stopped")
}

// IsDataServerHealthy 检查指定的 DataServer 是否健康
func (cs *ClusterService) IsDataServerHealthy(dataServerID string) bool {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	ds, exists := cs.dataServers[dataServerID]
	if !exists {
		return false
	}

	_, _, _, _, isHealthy := ds.GetStatus()
	return isHealthy
}

// GetDataServerAddresses 获取所有健康的 DataServer 地址列表
func (cs *ClusterService) GetDataServerAddresses() []string {
	healthyServers := cs.GetHealthyDataServers()

	var addresses []string
	for _, ds := range healthyServers {
		addresses = append(addresses, ds.Addr)
	}

	return addresses
}

// GetDataServerIDsByAddresses 根据地址列表获取对应的 DataServer ID 列表
func (cs *ClusterService) GetDataServerIDsByAddresses(addresses []string) []string {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	addrToID := make(map[string]string)
	for _, ds := range cs.dataServers {
		addrToID[ds.Addr] = ds.ID
	}

	var ids []string
	for _, addr := range addresses {
		if id, exists := addrToID[addr]; exists {
			ids = append(ids, id)
		}
	}

	return ids
}

// IsBlockReportedByServer 检查指定的DataServer是否报告了指定的块
func (cs *ClusterService) IsBlockReportedByServer(blockID uint64, serverID string) bool {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	ds, exists := cs.dataServers[serverID]
	if !exists {
		return false
	}

	return ds.HasBlock(blockID)
}

// triggerReplicaRedistribution 触发副本重分布
func (cs *ClusterService) triggerReplicaRedistribution(serverID, serverAddr string) {
	// 这个方法将被SchedulerService调用，这里先记录日志
	log.Printf("Triggering replica redistribution for permanently down server: %s (%s)", serverID, serverAddr)
	// 实际的重分布逻辑将在SchedulerService中实现
}

// GetPermanentlyDownServers 获取所有永久宕机的服务器
func (cs *ClusterService) GetPermanentlyDownServers() []*model.DataServerInfo {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	var downServers []*model.DataServerInfo
	for _, ds := range cs.dataServers {
		if ds.IsPermanentlyDownStatus() {
			downServers = append(downServers, ds)
		}
	}

	return downServers
}

// IsServerPermanentlyDown 检查特定服务器是否永久宕机
func (cs *ClusterService) IsServerPermanentlyDown(addr string) bool {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	for _, ds := range cs.dataServers {
		if ds.Addr == addr {
			return ds.IsPermanentlyDownStatus()
		}
	}
	return false
}

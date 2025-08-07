package service

import (
	"fmt"
	"log"
	"sync"
	"time"

	"metaserver/internal/model"
	"metaserver/pb"
)

type SchedulerService struct {
	config          *model.Config
	clusterService  *ClusterService
	metadataService *MetadataService
	
	// FSCK 相关
	fsckTicker *time.Ticker
	
	// 垃圾回收相关
	gcTicker *time.Ticker
	
	// 停止信号
	stopChan chan bool
	
	// 互斥锁
	mutex sync.RWMutex
	
	// 块 ID 生成器
	blockIDCounter uint64
	blockIDMutex   sync.Mutex
}

func NewSchedulerService(config *model.Config, clusterService *ClusterService, metadataService *MetadataService) *SchedulerService {
	ss := &SchedulerService{
		config:          config,
		clusterService:  clusterService,
		metadataService: metadataService,
		stopChan:        make(chan bool),
		blockIDCounter:  uint64(time.Now().Unix()), // 使用时间戳作为初始值
	}
	
	// 启动后台任务
	ss.startBackgroundTasks()
	
	return ss
}

// startBackgroundTasks 启动后台任务
func (ss *SchedulerService) startBackgroundTasks() {
	// 启动 FSCK
	ss.fsckTicker = time.NewTicker(ss.config.Scheduler.FSCKInterval)
	go ss.fsckLoop()
	
	// 启动垃圾回收
	ss.gcTicker = time.NewTicker(ss.config.Scheduler.GCInterval)
	go ss.gcLoop()
	
	log.Printf("Scheduler background tasks started (FSCK: %v, GC: %v)", 
		ss.config.Scheduler.FSCKInterval, ss.config.Scheduler.GCInterval)
}

// generateBlockID 生成唯一的块 ID
func (ss *SchedulerService) generateBlockID() uint64 {
	ss.blockIDMutex.Lock()
	defer ss.blockIDMutex.Unlock()
	
	ss.blockIDCounter++
	return ss.blockIDCounter
}

// AllocateBlocks 为文件分配数据块位置
func (ss *SchedulerService) AllocateBlocks(fileSize uint64, replication int) ([]*pb.BlockLocations, error) {
	if replication <= 0 {
		replication = ss.config.Cluster.DefaultReplication
	}
	
	// 计算需要的块数量
	blockCount := (fileSize + ss.config.Scheduler.BlockSize - 1) / ss.config.Scheduler.BlockSize
	if blockCount == 0 {
		blockCount = 1 // 至少分配一个块
	}
	
	var blockLocations []*pb.BlockLocations
	
	for i := uint64(0); i < blockCount; i++ {
		// 为每个块选择存储位置
		selectedServers, err := ss.clusterService.SelectDataServersForWrite(replication)
		if err != nil {
			return nil, fmt.Errorf("failed to select DataServers for block %d: %v", i, err)
		}
		
		// 生成块 ID
		blockID := ss.generateBlockID()
		
		// 构建位置列表
		var locations []string
		for _, server := range selectedServers {
			locations = append(locations, server.Addr)
		}
		
		blockLoc := &pb.BlockLocations{
			BlockId:   blockID,
			Locations: locations,
		}
		
		blockLocations = append(blockLocations, blockLoc)
		
		log.Printf("Allocated block %d with %d replicas: %v", blockID, len(locations), locations)
	}
	
	return blockLocations, nil
}

// AllocateBlocksRoundRobin 使用轮询方式分配数据块
func (ss *SchedulerService) AllocateBlocksRoundRobin(fileSize uint64, replication int) ([]*pb.BlockLocations, error) {
	if replication <= 0 {
		replication = ss.config.Cluster.DefaultReplication
	}
	
	blockCount := (fileSize + ss.config.Scheduler.BlockSize - 1) / ss.config.Scheduler.BlockSize
	if blockCount == 0 {
		blockCount = 1
	}
	
	var blockLocations []*pb.BlockLocations
	
	for i := uint64(0); i < blockCount; i++ {
		selectedServers, err := ss.clusterService.SelectDataServersRoundRobin(replication)
		if err != nil {
			return nil, fmt.Errorf("failed to select DataServers for block %d: %v", i, err)
		}
		
		blockID := ss.generateBlockID()
		
		var locations []string
		for _, server := range selectedServers {
			locations = append(locations, server.Addr)
		}
		
		blockLoc := &pb.BlockLocations{
			BlockId:   blockID,
			Locations: locations,
		}
		
		blockLocations = append(blockLocations, blockLoc)
	}
	
	return blockLocations, nil
}

// fsckLoop FSCK 循环检查
func (ss *SchedulerService) fsckLoop() {
	for {
		select {
		case <-ss.fsckTicker.C:
			ss.runFSCK()
		case <-ss.stopChan:
			return
		}
	}
}

// runFSCK 运行文件系统检查
func (ss *SchedulerService) runFSCK() {
	log.Println("Starting FSCK...")
	
	start := time.Now()
	repairedBlocks := 0
	orphanBlocks := 0
	
	// TODO: 实现完整的 FSCK 逻辑
	// 1. 从元数据中获取所有应该存在的块
	// 2. 从 DataServer 心跳中获取实际存在的块
	// 3. 比较差异并进行修复
	
	// 这里实现一个简化版本的 FSCK
	healthyServers := ss.clusterService.GetHealthyDataServers()
	if len(healthyServers) < ss.config.Cluster.DefaultReplication {
		log.Printf("FSCK warning: only %d healthy servers, cannot maintain %d replicas", 
			len(healthyServers), ss.config.Cluster.DefaultReplication)
	}
	
	duration := time.Since(start)
	log.Printf("FSCK completed in %v: repaired %d blocks, cleaned %d orphan blocks", 
		duration, repairedBlocks, orphanBlocks)
}

// gcLoop 垃圾回收循环
func (ss *SchedulerService) gcLoop() {
	for {
		select {
		case <-ss.gcTicker.C:
			ss.runGC()
		case <-ss.stopChan:
			return
		}
	}
}

// runGC 运行垃圾回收
func (ss *SchedulerService) runGC() {
	// 获取待回收的条目
	gcEntries, err := ss.metadataService.GetGCEntries()
	if err != nil {
		log.Printf("GC error: failed to get GC entries: %v", err)
		return
	}
	
	if len(gcEntries) == 0 {
		return
	}
	
	sentCount := 0
	confirmedCount := 0
	
	for _, entry := range gcEntries {
		switch entry.Status {
		case "pending":
			// 发送删除命令
			if ss.sendDeleteCommand(entry) {
				sentCount++
			}
			
		case "sent":
			// 检查删除是否完成
			if ss.checkDeletionComplete(entry) {
				// 删除完成，移除GC条目
				if err := ss.metadataService.RemoveGCEntry(entry.BlockID); err != nil {
					log.Printf("GC error: failed to remove GC entry for block %d: %v", entry.BlockID, err)
				} else {
					confirmedCount++
					log.Printf("GC completed for block %d", entry.BlockID)
				}
			} else {
				// 检查是否超时，如果超时则重新发送
				if time.Since(entry.SentTime) > 30*time.Second {
					log.Printf("GC timeout for block %d, resending delete command", entry.BlockID)
					if err := ss.metadataService.UpdateGCEntryStatus(entry.BlockID, "pending"); err != nil {
						log.Printf("Failed to reset GC status for block %d: %v", entry.BlockID, err)
					}
				}
			}
		}
	}
	
	if sentCount > 0 || confirmedCount > 0 {
		log.Printf("GC: sent %d delete commands, confirmed %d deletions", sentCount, confirmedCount)
	}
}

// sendDeleteCommand 发送删除命令
func (ss *SchedulerService) sendDeleteCommand(entry model.GCEntry) bool {
	dataServerIDs := ss.clusterService.GetDataServerIDsByAddresses(entry.Locations)
	
	if len(dataServerIDs) == 0 {
		log.Printf("GC warning: no healthy DataServers found for block %d locations: %v", 
			entry.BlockID, entry.Locations)
		return false
	}
	
	command := &model.Command{
		Action:  "DELETE_BLOCK",
		BlockID: entry.BlockID,
		Targets: entry.Locations,
	}
	
	ss.clusterService.SendCommandToMultiple(dataServerIDs, command)
	
	// 更新状态为已发送
	if err := ss.metadataService.UpdateGCEntryStatus(entry.BlockID, "sent"); err != nil {
		log.Printf("Failed to update GC status for block %d: %v", entry.BlockID, err)
		return false
	}
	
	log.Printf("GC: sent delete command for block %d to %d DataServers", entry.BlockID, len(dataServerIDs))
	return true
}

// checkDeletionComplete 检查删除是否完成
func (ss *SchedulerService) checkDeletionComplete(entry model.GCEntry) bool {
	// 检查所有相关的DataServer是否还在报告这个块
	for _, location := range entry.Locations {
		dataServer := ss.clusterService.GetDataServerByAddr(location)
		if dataServer == nil {
			// DataServer离线，认为删除完成
			continue
		}
		
		// 检查DataServer是否还在报告这个块
		if ss.clusterService.IsBlockReportedByServer(entry.BlockID, dataServer.ID) {
			// 还在报告这个块，删除未完成
			return false
		}
	}
	
	// 所有DataServer都不再报告这个块，删除完成
	return true
}

// ScheduleBlockDeletion 调度块删除
func (ss *SchedulerService) ScheduleBlockDeletion(blockID uint64, locations []string) {
	// 添加到垃圾回收队列
	err := ss.metadataService.AddGCEntry(blockID, locations)
	if err != nil {
		log.Printf("Failed to add GC entry for block %d: %v", blockID, err)
		return
	}
	
	log.Printf("Block %d scheduled for deletion from locations: %v", blockID, locations)
}

// ScheduleBlockReplication 调度块副本恢复
func (ss *SchedulerService) ScheduleBlockReplication(blockID uint64, sourceAddr string, targetAddrs []string) {
	// 获取源和目标 DataServer ID
	sourceDS := ss.clusterService.GetDataServerByAddr(sourceAddr)
	if sourceDS == nil {
		log.Printf("Source DataServer not found for address %s", sourceAddr)
		return
	}
	
	for _, targetAddr := range targetAddrs {
		targetDS := ss.clusterService.GetDataServerByAddr(targetAddr)
		if targetDS == nil {
			log.Printf("Target DataServer not found for address %s", targetAddr)
			continue
		}
		
		command := &model.Command{
			Action:  "COPY_BLOCK",
			BlockID: blockID,
			Targets: []string{sourceAddr}, // 对于 COPY_BLOCK，targets 表示源地址
		}
		
		ss.clusterService.SendCommand(targetDS.ID, command)
		
		log.Printf("Block %d replication scheduled from %s to %s", blockID, sourceAddr, targetAddr)
	}
}

// GetOptimalDataServers 获取最优的 DataServer 列表（负载均衡）
func (ss *SchedulerService) GetOptimalDataServers(count int) ([]*model.DataServerInfo, error) {
	return ss.clusterService.SelectDataServersForWrite(count)
}

// GetBlockDistribution 获取块分布统计
func (ss *SchedulerService) GetBlockDistribution() map[string]uint64 {
	distribution := make(map[string]uint64)
	
	servers := ss.clusterService.GetAllDataServers()
	for _, server := range servers {
		blockCount, _, _, _ := server.GetStatus()
		distribution[server.ID] = blockCount
	}
	
	return distribution
}

// RebalanceCluster 集群重平衡（简化实现）
func (ss *SchedulerService) RebalanceCluster() error {
	log.Println("Starting cluster rebalancing...")
	
	distribution := ss.GetBlockDistribution()
	
	// 计算平均块数
	var totalBlocks uint64
	serverCount := len(distribution)
	
	if serverCount == 0 {
		return fmt.Errorf("no DataServers available")
	}
	
	for _, blockCount := range distribution {
		totalBlocks += blockCount
	}
	
	avgBlocks := totalBlocks / uint64(serverCount)
	
	log.Printf("Cluster stats: %d servers, %d total blocks, %.1f avg blocks per server", 
		serverCount, totalBlocks, float64(avgBlocks))
	
	// 简化实现：只打印统计信息
	// 实际的重平衡需要复杂的算法和数据迁移
	
	return nil
}

// GetReplicationStatus 获取副本状态统计
func (ss *SchedulerService) GetReplicationStatus() (underReplicated, overReplicated, healthy int) {
	// TODO: 实现副本状态检查
	// 这需要遍历所有文件的块映射并检查每个块的副本数
	
	return 0, 0, 0
}

// Stop 停止调度服务
func (ss *SchedulerService) Stop() {
	if ss.fsckTicker != nil {
		ss.fsckTicker.Stop()
	}
	
	if ss.gcTicker != nil {
		ss.gcTicker.Stop()
	}
	
	// 发送停止信号
	select {
	case ss.stopChan <- true:
	case ss.stopChan <- true: // 发送两次，给两个 goroutine
	default:
	}
	
	log.Println("SchedulerService stopped")
}

// GetSchedulerStats 获取调度器统计信息
func (ss *SchedulerService) GetSchedulerStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	// 获取集群统计
	totalServers, healthyServers, totalBlocks, totalFreeSpace := ss.clusterService.GetClusterStats()
	
	stats["total_servers"] = totalServers
	stats["healthy_servers"] = healthyServers
	stats["total_blocks"] = totalBlocks
	stats["total_free_space_mb"] = totalFreeSpace / 1024 / 1024
	stats["block_size_mb"] = ss.config.Scheduler.BlockSize / 1024 / 1024
	stats["default_replication"] = ss.config.Cluster.DefaultReplication
	
	// 获取副本状态
	under, over, healthy := ss.GetReplicationStatus()
	stats["under_replicated_blocks"] = under
	stats["over_replicated_blocks"] = over
	stats["healthy_blocks"] = healthy
	
	return stats
}

// ValidateClusterHealth 验证集群健康状况
func (ss *SchedulerService) ValidateClusterHealth() []string {
	var issues []string
	
	totalServers, healthyServers, _, totalFreeSpace := ss.clusterService.GetClusterStats()
	
	if totalServers == 0 {
		issues = append(issues, "No DataServers registered")
	}
	
	if healthyServers < ss.config.Cluster.DefaultReplication {
		issues = append(issues, fmt.Sprintf("Not enough healthy servers (%d) to maintain replication (%d)", 
			healthyServers, ss.config.Cluster.DefaultReplication))
	}
	
	if totalFreeSpace < ss.config.Scheduler.BlockSize {
		issues = append(issues, "Cluster running low on storage space")
	}
	
	return issues
}

// ForceGC 强制执行垃圾回收
func (ss *SchedulerService) ForceGC() {
	go ss.runGC()
}

// ForceFSCK 强制执行文件系统检查
func (ss *SchedulerService) ForceFSCK() {
	go ss.runFSCK()
}
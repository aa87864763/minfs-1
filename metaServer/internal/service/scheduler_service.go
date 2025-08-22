package service

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"metaServer/internal/model"
	"metaServer/pb"
	
	"github.com/dgraph-io/badger/v3"
	"google.golang.org/protobuf/proto"
)

// 使用 model.RepairTask 替代本地定义

type SchedulerService struct {
	config          *model.Config
	clusterService  *ClusterService
	metadataService *MetadataService
	
	// FSCK 相关
	fsckTicker *time.Ticker
	
	// 垃圾回收相关
	gcTicker *time.Ticker
	
	// 结束信号
	stopChan chan bool
	
	// 互斥锁
	mutex sync.RWMutex

	// 正在修复的块跟踪
	repairingBlocks map[uint64][]model.RepairTask // blockID -> 修复任务列表
	repairMutex     sync.RWMutex
	
	// Worker Pool 相关
	fsckCheckQueue    chan *model.FSCKCheckTask // FSCK检查任务队列
	repairTaskQueue   chan *model.RepairTask    // 修复任务队列
	concurrentRepairs int32                     // 当前并发修复数
	maxConcurrentRepairs int32                  // 最大并发修复数
	
	// 块ID生成相关
	lastTimestamp int64 // 上次生成ID的时间戳
	counter       int64 // 当前时间戳下的计数器
	idMutex      sync.Mutex // ID生成互斥锁
}

func NewSchedulerService(config *model.Config, clusterService *ClusterService, metadataService *MetadataService) *SchedulerService {
	ss := &SchedulerService{
		config:          config,
		clusterService:  clusterService,
		metadataService: metadataService,
		stopChan:        make(chan bool),
		repairingBlocks: make(map[uint64][]model.RepairTask),
		
		// 初始化Worker Pool
		fsckCheckQueue:       make(chan *model.FSCKCheckTask, config.Scheduler.RepairQueueSize),
		repairTaskQueue:      make(chan *model.RepairTask, config.Scheduler.RepairQueueSize),
		maxConcurrentRepairs: int32(config.Scheduler.MaxConcurrentRepairs),
		
		// 初始化ID生成相关字段
		lastTimestamp: 0,
		counter:       0,
	}
	
	// 设置块复制完成回调
	clusterService.SetReplicationCallback(ss.OnBlockReplicationComplete)
	
	// 启动后台任务
	ss.startBackgroundTasks()
	
	return ss
}

// startBackgroundTasks 启动后台任务
func (ss *SchedulerService) startBackgroundTasks() {
	// 启动FSCK Worker Pool
	ss.startFSCKWorkerPool()
	
	// 启动修复任务Worker Pool  
	ss.startRepairWorkerPool()
	
	// 启动 FSCK
	ss.fsckTicker = time.NewTicker(ss.config.Scheduler.FSCKInterval)
	go ss.fsckLoop()
	
	// 启动垃圾回收
	ss.gcTicker = time.NewTicker(ss.config.Scheduler.GCInterval)
	go ss.gcLoop()
	
	log.Printf("Scheduler background tasks started (FSCK: %v, GC: %v)", 
		ss.config.Scheduler.FSCKInterval, ss.config.Scheduler.GCInterval)
}

// generateBlockID 生成唯一的块 ID（实时时间戳 + 累加数）
func (ss *SchedulerService) generateBlockID() (uint64, error) {
    ss.idMutex.Lock()
    defer ss.idMutex.Unlock()
    
    // 获取当前毫秒时间戳
    currentTimestamp := time.Now().UnixNano() / 1e6
    
    // 如果时间戳变化了，重置计数器
    if currentTimestamp != ss.lastTimestamp {
        ss.lastTimestamp = currentTimestamp
        ss.counter = 1 // 从1开始计数
    } else {
        // 同一毫秒内，计数器递增
        ss.counter++
    }
    
    // 生成最终ID：时间戳 + 3位计数器（001-100）
    blockID := uint64(currentTimestamp)*1000 + uint64(ss.counter)
    
    log.Printf("Generated block ID: %d (timestamp: %d, counter: %d)", 
        blockID, currentTimestamp, ss.counter)
    
    return blockID, nil
}

// AllocateBlocks 为文件分配数据块位置（改进的按块调度策略）
func (ss *SchedulerService) AllocateBlocks(fileSize uint64, replication int) ([]*pb.BlockLocations, error) {
	if replication <= 0 {
		replication = ss.config.Cluster.DefaultReplication
	}
	
	// 计算需要的逻辑块数量
	logicalBlockCount := (fileSize + ss.config.Scheduler.BlockSize - 1) / ss.config.Scheduler.BlockSize
	if logicalBlockCount == 0 {
		logicalBlockCount = 1 // 至少分配一个块
	}
	
	// 计算总的物理块数量（逻辑块数 × 副本数）
	totalPhysicalBlocks := int(logicalBlockCount) * replication
	
	log.Printf("File allocation: size=%d bytes, logical blocks=%d, replication=%d, total physical blocks=%d",
		fileSize, logicalBlockCount, replication, totalPhysicalBlocks)
	
	// 获取所有物理块的分布（轮询分配到所有DataServer）
	physicalBlockAssignments, err := ss.clusterService.SelectDataServersForThreeReplica(totalPhysicalBlocks)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate physical blocks: %v", err)
	}
	
	var blockLocations []*pb.BlockLocations
	
	// 为每个逻辑块创建其副本分布
	for i := uint64(0); i < logicalBlockCount; i++ {
        // 生成逻辑块 ID（基于毫秒时间戳）
        blockID, err := ss.generateBlockID()
        if err != nil {
            return nil, fmt.Errorf("failed to generate block ID: %v", err)
        }
        
        // 为当前逻辑块分配副本位置
        var locations []string
        for j := 0; j < replication; j++ {
            physicalBlockIndex := int(i)*replication + j
            if physicalBlockIndex < len(physicalBlockAssignments) {
                locations = append(locations, physicalBlockAssignments[physicalBlockIndex])
            }
        }
        
        blockLoc := &pb.BlockLocations{
            BlockId:   blockID,
            Locations: locations,
        }
        
        blockLocations = append(blockLocations, blockLoc)
        
        log.Printf("Logical block %d (ID: %d) with %d replicas distributed at: %v",
            i, blockID, len(locations), locations)
    }
	
	log.Printf("Successfully allocated %d logical blocks with %d replicas each, total %d physical blocks distributed across DataServers",
		logicalBlockCount, replication, totalPhysicalBlocks)
	
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
	cleanedOrphanBlocks := 0
	orphanBlocks := 0
	underReplicatedBlocks := 0
	redistributedBlocks := 0
	
	// 检查集群健康状况
	healthyServers := ss.clusterService.GetHealthyDataServers()
	if len(healthyServers) < ss.config.Cluster.DefaultReplication {
		log.Printf("FSCK warning: only %d healthy servers, cannot maintain %d replicas", 
			len(healthyServers), ss.config.Cluster.DefaultReplication)
		return
	}
	
	// 处理永久宕机节点的副本重分布
	permanentlyDownServers := ss.clusterService.GetPermanentlyDownServers()
	if len(permanentlyDownServers) > 0 {
		redistributedCount := ss.handlePermanentlyDownServers(permanentlyDownServers)
		redistributedBlocks += redistributedCount
	}
	
	// 1. 获取所有应该存在的块（从元数据）
	expectedBlocks, err := ss.getAllExpectedBlocks()
	if err != nil {
		log.Printf("FSCK error: failed to get expected blocks: %v", err)
		return
	}
	
	// 2. 获取实际存在的块（从DataServer心跳报告）
	actualBlocks := ss.getAllActualBlocks()
	
	log.Printf("FSCK: checking %d expected blocks against actual blocks from %d servers", 
		len(expectedBlocks), len(healthyServers))
	
	// 3. 使用Worker Pool并行处理所有块检查
	log.Printf("FSCK: Distributing %d blocks to worker pool for parallel checking", len(expectedBlocks))
	
	// 将所有块检查任务分发给worker pool
	tasksSubmitted := 0
	for blockID, expectedLocations := range expectedBlocks {
		actualLocations := actualBlocks[blockID]
		
		// 创建FSCK检查任务
		checkTask := &model.FSCKCheckTask{
			BlockID:           blockID,
			ExpectedLocations: expectedLocations,
			ActualLocations:   actualLocations,
		}
		
		// 提交任务到worker pool
		select {
		case ss.fsckCheckQueue <- checkTask:
			tasksSubmitted++
		default:
			log.Printf("FSCK: Check queue full, processing block %d inline", blockID)
			// 队列满时，直接处理
			ss.processFSCKCheckTask(checkTask, -1) // -1表示主线程处理
		}
	}
	
	log.Printf("FSCK: Submitted %d check tasks to worker pool", tasksSubmitted)
	
	// 4. 检查完全孤儿的块（只在DataServer存在，元数据中完全没有的块）
	for blockID, actualLocations := range actualBlocks {
		if _, exists := expectedBlocks[blockID]; !exists {
			orphanBlocks++
			log.Printf("FSCK: Found orphan block %d at locations: %v", blockID, actualLocations)
			
			// 立即清理孤儿块
			log.Printf("FSCK: Scheduling deletion of orphan block %d", blockID)
			ss.ScheduleBlockDeletion(blockID, actualLocations)
			cleanedOrphanBlocks++ // 计入清理数量
		}
	}
	
	duration := time.Since(start)
	log.Printf("FSCK completed in %v: checked %d blocks, repaired %d under-replicated, found %d orphans, cleaned %d orphans, redistributed %d blocks from down servers", 
		duration, len(expectedBlocks), repairedBlocks, orphanBlocks, cleanedOrphanBlocks, redistributedBlocks)
	
	if underReplicatedBlocks > 0 {
		log.Printf("FSCK: %d blocks are under-replicated and need attention", underReplicatedBlocks)
	}
}

// getAllExpectedBlocks 从元数据中获取所有应该存在的块及其位置
func (ss *SchedulerService) getAllExpectedBlocks() (map[uint64][]string, error) {
	expectedBlocks := make(map[uint64][]string)
	
	log.Println("FSCK: Collecting expected blocks from metadata...")
	
	// 这里实现遍历所有文件并获取其块映射的逻辑
	// 由于BadgerDB的特性，我们需要遍历所有的inode条目
	err := ss.metadataService.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		
		// 遍历所有 inode 条目
		inodePrefix := []byte(model.PrefixInode)
		for it.Seek(inodePrefix); it.ValidForPrefix(inodePrefix); it.Next() {
			item := it.Item()
			
			err := item.Value(func(val []byte) error {
				var nodeInfo pb.NodeInfo
				if err := proto.Unmarshal(val, &nodeInfo); err != nil {
					log.Printf("FSCK warning: failed to unmarshal inode: %v", err)
					return nil // 继续处理下一个
				}
				
				// 只处理文件类型
				if nodeInfo.Type != pb.FileType_File {
					return nil
				}
				
				// 获取该文件的块映射
				blockMappings, err := ss.metadataService.GetBlockMappings(nodeInfo.Inode)
				if err != nil {
					log.Printf("FSCK warning: failed to get block mappings for inode %d: %v", nodeInfo.Inode, err)
					return nil
				}
				
				// 添加到期望块列表中
				for _, blockMapping := range blockMappings {
					expectedBlocks[blockMapping.BlockId] = blockMapping.Locations
				}
				
				return nil
			})
			
			if err != nil {
				log.Printf("FSCK warning: error processing inode: %v", err)
			}
		}
		
		return nil
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to traverse metadata: %v", err)
	}
	
	log.Printf("FSCK: Found %d expected blocks in metadata", len(expectedBlocks))
	return expectedBlocks, nil
}

// getAllActualBlocks 从DataServer心跳报告中获取实际存在的块
func (ss *SchedulerService) getAllActualBlocks() map[uint64][]string {
	actualBlocks := make(map[uint64][]string)
	
	healthyServers := ss.clusterService.GetHealthyDataServers()
	
	for _, server := range healthyServers {
		// 获取服务器上报告的所有块
		reportedBlocks := server.GetReportedBlocks()
		
		for blockID := range reportedBlocks {
			if actualBlocks[blockID] == nil {
				actualBlocks[blockID] = make([]string, 0)
			}
			actualBlocks[blockID] = append(actualBlocks[blockID], server.Addr)
		}
	}
	
	return actualBlocks
}

// findMissingReplicas 找出缺失的副本位置
func (ss *SchedulerService) findMissingReplicas(expectedLocations, actualLocations []string) []string {
	var missing []string
	
	for _, expected := range expectedLocations {
		found := false
		for _, actual := range actualLocations {
			if expected == actual {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, expected)
		}
	}
	
	return missing
}

// 已删除findMissingReplicasWithRepairTracking，使用findMissingReplicas替代

// isLocationInExpected 检查位置是否在预期列表中
func (ss *SchedulerService) isLocationInExpected(location string, expectedLocations []string) bool {
	for _, expected := range expectedLocations {
		if location == expected {
			return true
		}
	}
	return false
}

// 已删除repairUnderReplicatedBlock，使用Worker Pool架构中的generateRepairTask+processRepairTask替代

// ReplacementUpdate 服务器替换更新信息
type ReplacementUpdate struct {
	BlockID uint64
	OldAddr string
	NewAddr string
}

// applyReplacementUpdates 应用服务器替换的元数据更新
func (ss *SchedulerService) applyReplacementUpdates(updates []ReplacementUpdate) {
	for _, update := range updates {
		err := ss.metadataService.UpdateBlockLocation(update.BlockID, update.OldAddr, update.NewAddr)
		if err != nil {
			log.Printf("Failed to update metadata for block %d replacement (%s -> %s): %v", 
				update.BlockID, update.OldAddr, update.NewAddr, err)
		} else {
			log.Printf("Updated metadata for block %d: replaced %s with %s", 
				update.BlockID, update.OldAddr, update.NewAddr)
		}
	}
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
	
	// 立即执行删除操作，而不是等待GC周期
	go ss.executeImmediateBlockDeletion(blockID, locations)
}

// executeImmediateBlockDeletion 立即执行块删除操作
func (ss *SchedulerService) executeImmediateBlockDeletion(blockID uint64, locations []string) {
	log.Printf("Executing immediate deletion for block %d", blockID)
	
	// 获取相应的DataServer ID
	dataServerIDs := ss.clusterService.GetDataServerIDsByAddresses(locations)
	
	if len(dataServerIDs) == 0 {
		log.Printf("Warning: no healthy DataServers found for block %d locations: %v", blockID, locations)
		return
	}
	
	// 创建删除命令
	command := &model.Command{
		Action:  "DELETE_BLOCK",
		BlockID: blockID,
		Targets: locations,
	}
	
	// 发送删除命令到所有相关的DataServer
	ss.clusterService.SendCommandToMultiple(dataServerIDs, command)
	
	// 更新GC条目状态为已发送
	if err := ss.metadataService.UpdateGCEntryStatus(blockID, "sent"); err != nil {
		log.Printf("Failed to update GC status for block %d: %v", blockID, err)
	}
	
	log.Printf("Immediate delete command sent for block %d to %d DataServers", blockID, len(dataServerIDs))
}

// ScheduleBlockReplication 调度块副本恢复（普通修复）
func (ss *SchedulerService) ScheduleBlockReplication(blockID uint64, sourceAddr string, targetAddrs []string) {
	ss.scheduleBlockReplication(blockID, sourceAddr, targetAddrs, false, "")
}

// ScheduleBlockReplicationWithReplacement 调度块副本恢复（替换操作）
func (ss *SchedulerService) ScheduleBlockReplicationWithReplacement(blockID uint64, sourceAddr string, targetAddr string, replacedAddr string) {
	ss.scheduleBlockReplication(blockID, sourceAddr, []string{targetAddr}, true, replacedAddr)
}

// scheduleBlockReplication 内部调度块副本恢复方法
func (ss *SchedulerService) scheduleBlockReplication(blockID uint64, sourceAddr string, targetAddrs []string, isReplacement bool, replacedAddr string) {
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
		
		// 记录修复任务（包含替换信息）
		ss.addRepairTaskWithReplacement(blockID, sourceAddr, targetAddr, isReplacement, replacedAddr)
		
		command := &model.Command{
			Action:  "COPY_BLOCK",
			BlockID: blockID,
			Targets: []string{sourceAddr}, // 对于 COPY_BLOCK，targets 表示源地址
		}
		
		ss.clusterService.SendCommand(targetDS.ID, command)
		
		if isReplacement {
			log.Printf("Block %d replacement scheduled from %s to %s (replacing %s)", blockID, sourceAddr, targetAddr, replacedAddr)
		} else {
			log.Printf("Block %d replication scheduled from %s to %s", blockID, sourceAddr, targetAddr)
		}
	}
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


// ForceGC 强制执行垃圾回收
func (ss *SchedulerService) ForceGC() {
	go ss.runGC()
}

// ForceFSCK 强制执行文件系统检查
func (ss *SchedulerService) ForceFSCK() {
	go ss.runFSCK()
}

// addRepairTask 添加修复任务跟踪（普通修复）
func (ss *SchedulerService) addRepairTask(blockID uint64, sourceAddr, targetAddr string) {
	ss.addRepairTaskWithReplacement(blockID, sourceAddr, targetAddr, false, "")
}

// addRepairTaskWithReplacement 添加修复任务跟踪（支持替换）
func (ss *SchedulerService) addRepairTaskWithReplacement(blockID uint64, sourceAddr, targetAddr string, isReplacement bool, replacedAddr string) {
	ss.repairMutex.Lock()
	defer ss.repairMutex.Unlock()
	
	task := model.RepairTask{
		BlockID:       blockID,
		SourceAddr:    sourceAddr,
		TargetAddrs:   []string{targetAddr}, // 转换为数组
		IsReplacement: isReplacement,
		ReplacedAddr:  replacedAddr,
		Priority:      1,
	}
	
	ss.repairingBlocks[blockID] = append(ss.repairingBlocks[blockID], task)
	
	if isReplacement {
		log.Printf("Added replacement task: block %d from %s to %s (replacing %s)", blockID, sourceAddr, targetAddr, replacedAddr)
	} else {
		log.Printf("Added repair task: block %d from %s to %s", blockID, sourceAddr, targetAddr)
	}
}

// removeRepairTask 移除修复任务跟踪
func (ss *SchedulerService) removeRepairTask(blockID uint64, targetAddr string) {
	ss.repairMutex.Lock()
	defer ss.repairMutex.Unlock()
	
	tasks := ss.repairingBlocks[blockID]
	for i, task := range tasks {
		// 检查目标地址是否在任务的目标列表中
		for _, addr := range task.TargetAddrs {
			if addr == targetAddr {
				// 移除该任务
				ss.repairingBlocks[blockID] = append(tasks[:i], tasks[i+1:]...)
				log.Printf("Completed repair task: block %d to %s", blockID, targetAddr)
				break
			}
		}
	}
	
	// 如果该块的所有修复任务都完成了，删除该条目
	if len(ss.repairingBlocks[blockID]) == 0 {
		delete(ss.repairingBlocks, blockID)
	}
}

// getRepairingTargets 获取正在修复到的目标地址列表
func (ss *SchedulerService) getRepairingTargets(blockID uint64) []string {
	ss.repairMutex.RLock()
	defer ss.repairMutex.RUnlock()
	
	var targets []string
	for _, task := range ss.repairingBlocks[blockID] {
		targets = append(targets, task.TargetAddrs...)
	}
	return targets
}

// getRepairTask 获取指定的修复任务
func (ss *SchedulerService) getRepairTask(blockID uint64, targetAddr string) *model.RepairTask {
	ss.repairMutex.RLock()
	defer ss.repairMutex.RUnlock()
	
	for _, task := range ss.repairingBlocks[blockID] {
		for _, addr := range task.TargetAddrs {
			if addr == targetAddr {
				return &task
			}
		}
	}
	return nil
}

// OnBlockReplicationComplete 当DataServer完成块复制时调用
func (ss *SchedulerService) OnBlockReplicationComplete(blockID uint64, targetAddr string, success bool) {
	if success {
		// 检查是否是替换操作，如果是则更新元数据
		task := ss.getRepairTask(blockID, targetAddr)
		if task != nil && task.IsReplacement {
			// 这是一个替换操作，更新元数据
			err := ss.metadataService.UpdateBlockLocation(blockID, task.ReplacedAddr, targetAddr)
			if err != nil {
				log.Printf("Failed to update metadata after replacement for block %d (%s -> %s): %v", 
					blockID, task.ReplacedAddr, targetAddr, err)
			} else {
				log.Printf("Updated metadata after replacement: block %d from %s to %s", 
					blockID, task.ReplacedAddr, targetAddr)
			}
		}
		
		ss.removeRepairTask(blockID, targetAddr)
		log.Printf("Block %d replication to %s completed successfully", blockID, targetAddr)
	} else {
		ss.removeRepairTask(blockID, targetAddr)
		log.Printf("Block %d replication to %s failed", blockID, targetAddr)
		// 失败的情况下，可以选择重新调度或保持任务状态等待重试
	}
}

// handlePermanentlyDownServers 处理永久宕机服务器的副本重分布
func (ss *SchedulerService) handlePermanentlyDownServers(downServers []*model.DataServerInfo) int {
	redistributedCount := 0
	
	for _, downServer := range downServers {
		log.Printf("Processing permanently down server: %s (%s)", downServer.ID, downServer.Addr)
		
		// 获取该服务器上的所有块
		affectedBlocks, err := ss.getBlocksOnServer(downServer.Addr)
		if err != nil {
			log.Printf("Failed to get blocks on server %s: %v", downServer.Addr, err)
			continue
		}
		
		log.Printf("Found %d blocks on down server %s", len(affectedBlocks), downServer.Addr)
		
		// 为每个受影响的块进行重分布
		for _, blockID := range affectedBlocks {
			if ss.redistributeBlockFromDownServer(blockID, downServer.Addr) {
				redistributedCount++
			}
		}
	}
	
	return redistributedCount
}

// getBlocksOnServer 获取指定服务器上的所有块ID
func (ss *SchedulerService) getBlocksOnServer(serverAddr string) ([]uint64, error) {
	var blocksOnServer []uint64
	
	// 从元数据中查找所有包含该服务器地址的块
	expectedBlocks, err := ss.getAllExpectedBlocks()
	if err != nil {
		return nil, fmt.Errorf("failed to get expected blocks: %v", err)
	}
	
	for blockID, locations := range expectedBlocks {
		for _, location := range locations {
			if location == serverAddr {
				blocksOnServer = append(blocksOnServer, blockID)
				break
			}
		}
	}
	
	return blocksOnServer, nil
}

// redistributeBlockFromDownServer 为宕机服务器上的块重新分布副本
func (ss *SchedulerService) redistributeBlockFromDownServer(blockID uint64, downServerAddr string) bool {
	// 获取当前块的所有位置
	expectedBlocks, err := ss.getAllExpectedBlocks()
	if err != nil {
		log.Printf("Failed to get expected blocks for redistribution of block %d: %v", blockID, err)
		return false
	}
	
	expectedLocations, exists := expectedBlocks[blockID]
	if !exists {
		log.Printf("Block %d not found in metadata, skipping redistribution", blockID)
		return false
	}
	
	// 获取实际存在的位置（排除宕机服务器）
	actualBlocks := ss.getAllActualBlocks()
	actualLocations := actualBlocks[blockID]
	
	var availableLocations []string
	for _, location := range actualLocations {
		if location != downServerAddr && !ss.clusterService.IsServerPermanentlyDown(location) {
			availableLocations = append(availableLocations, location)
		}
	}
	
	if len(availableLocations) == 0 {
		log.Printf("No available source locations for block %d", blockID)
		return false
	}
	
	// 选择一个新的健康节点作为目标
	newTargetServer, err := ss.selectBestTargetServer(expectedLocations)
	if err != nil {
		log.Printf("Failed to select target server for block %d: %v", blockID, err)
		return false
	}
	
	// 选择源服务器（从可用位置中选择第一个）
	sourceAddr := availableLocations[0]
	
	log.Printf("Redistributing block %d from down server %s to new server %s (source: %s)", 
		blockID, downServerAddr, newTargetServer.Addr, sourceAddr)
	
	// 调度块复制（元数据更新将在复制完成后进行）
	ss.ScheduleBlockReplicationWithReplacement(blockID, sourceAddr, newTargetServer.Addr, downServerAddr)
	
	return true
}

// selectBestTargetServer 为副本重分布选择最佳目标服务器
func (ss *SchedulerService) selectBestTargetServer(excludeAddrs []string) (*model.DataServerInfo, error) {
	healthyServers := ss.clusterService.GetHealthyDataServers()
	
	// 过滤掉已经包含该块的服务器
	excludeSet := make(map[string]bool)
	for _, addr := range excludeAddrs {
		excludeSet[addr] = true
	}
	
	var candidateServers []*model.DataServerInfo
	for _, server := range healthyServers {
		if !excludeSet[server.Addr] && !server.IsPermanentlyDownStatus() {
			candidateServers = append(candidateServers, server)
		}
	}
	
	if len(candidateServers) == 0 {
		return nil, fmt.Errorf("no suitable target servers available")
	}
	
	// 选择块数量最少的服务器（负载均衡）
	sort.Slice(candidateServers, func(i, j int) bool {
		blockCountI, _, _, _, _ := candidateServers[i].GetStatus()
		blockCountJ, _, _, _, _ := candidateServers[j].GetStatus()
		return blockCountI < blockCountJ
	})
	
	return candidateServers[0], nil
}

// startFSCKWorkerPool 启动FSCK检查工作协程池
func (ss *SchedulerService) startFSCKWorkerPool() {
	workerCount := ss.config.Scheduler.FSCKWorkers
	log.Printf("Starting %d FSCK check workers", workerCount)
	
	for i := 0; i < workerCount; i++ {
		go ss.fsckCheckWorker(i)
	}
}

// startRepairWorkerPool 启动修复任务工作协程池
func (ss *SchedulerService) startRepairWorkerPool() {
	workerCount := ss.config.Scheduler.RepairWorkers
	log.Printf("Starting %d repair workers (max concurrent: %d)", 
		workerCount, ss.maxConcurrentRepairs)
	
	for i := 0; i < workerCount; i++ {
		go ss.repairWorker(i)
	}
}

// fsckCheckWorker FSCK检查工作协程
func (ss *SchedulerService) fsckCheckWorker(workerID int) {
	log.Printf("FSCK check worker %d started", workerID)
	
	for {
		select {
		case task := <-ss.fsckCheckQueue:
			if task == nil {
				log.Printf("FSCK check worker %d stopped", workerID)
				return
			}
			
			ss.processFSCKCheckTask(task, workerID)
			
		case <-ss.stopChan:
			log.Printf("FSCK check worker %d stopped", workerID)
			return
		}
	}
}

// repairWorker 修复任务工作协程
func (ss *SchedulerService) repairWorker(workerID int) {
	log.Printf("Repair worker %d started", workerID)
	
	for {
		select {
		case task := <-ss.repairTaskQueue:
			if task == nil {
				log.Printf("Repair worker %d stopped", workerID)
				return
			}
			
			// 检查并发限制
			if atomic.LoadInt32(&ss.concurrentRepairs) >= ss.maxConcurrentRepairs {
				// 达到并发限制，重新入队等待
				select {
				case ss.repairTaskQueue <- task:
					time.Sleep(100 * time.Millisecond)
					continue
				default:
					log.Printf("Repair queue full, dropping task for block %d", task.BlockID)
					continue
				}
			}
			
			ss.processRepairTask(task, workerID)
			
		case <-ss.stopChan:
			log.Printf("Repair worker %d stopped", workerID)
			return
		}
	}
}

// processFSCKCheckTask 处理FSCK检查任务
func (ss *SchedulerService) processFSCKCheckTask(task *model.FSCKCheckTask, workerID int) {
	blockID := task.BlockID
	expectedLocations := task.ExpectedLocations
	actualLocations := task.ActualLocations
	
	// 检查副本不足的情况
	missingLocations := ss.findMissingReplicas(expectedLocations, actualLocations)
	if len(missingLocations) > 0 {
		log.Printf("Worker %d: Block %d is under-replicated, missing from %d servers: %v", 
			workerID, blockID, len(missingLocations), missingLocations)
		
		// 生成修复任务
		repairTask := ss.generateRepairTask(blockID, expectedLocations, actualLocations, missingLocations)
		if repairTask != nil {
			// 提交到修复任务队列
			select {
			case ss.repairTaskQueue <- repairTask:
				log.Printf("Worker %d: Submitted repair task for block %d", workerID, blockID)
			default:
				log.Printf("Worker %d: Repair queue full, dropping task for block %d", workerID, blockID)
			}
		}
		// 如果块正在被修复，则不应将其任何副本视为孤儿块
		return
	}
	
	// 只有在副本数正常的情况下，才检查并清理孤儿块
	for _, location := range actualLocations {
		if !ss.isLocationInExpected(location, expectedLocations) {
			log.Printf("Worker %d: Found orphan block %d at %s, scheduling deletion", workerID, blockID, location)
			// 立即删除这个孤儿副本
			ss.ScheduleBlockDeletion(blockID, []string{location})
		}
	}
}

// processRepairTask 处理修复任务
func (ss *SchedulerService) processRepairTask(task *model.RepairTask, workerID int) {
	atomic.AddInt32(&ss.concurrentRepairs, 1)
	defer atomic.AddInt32(&ss.concurrentRepairs, -1)
	
	log.Printf("Repair worker %d: Processing block %d from %s to %v", 
		workerID, task.BlockID, task.SourceAddr, task.TargetAddrs)
	
	// 执行实际的修复调度
	ss.scheduleBlockReplication(task.BlockID, task.SourceAddr, task.TargetAddrs, task.IsReplacement, task.ReplacedAddr)
	
	// 如果是替换操作，直接更新元数据（不等待回调，因为孤儿块已存在）
	if task.IsReplacement && task.ReplacedAddr != "" {
		for _, targetAddr := range task.TargetAddrs {
			// 添加重试机制处理Transaction Conflict
			var err error
			for retries := 0; retries < 3; retries++ {
				err = ss.metadataService.UpdateBlockLocation(task.BlockID, task.ReplacedAddr, targetAddr)
				if err == nil {
					log.Printf("Repair worker %d: Updated metadata for block %d (%s -> %s)", 
						workerID, task.BlockID, task.ReplacedAddr, targetAddr)
					break // 成功，退出重试循环
				}
				
				// 检查是否是事务冲突，如果是则重试
				if strings.Contains(err.Error(), "Transaction Conflict") {
					backoff := time.Duration(retries*10+5) * time.Millisecond // 指数退避: 5ms, 15ms, 25ms
					log.Printf("Repair worker %d: Transaction conflict for block %d, retrying in %v (attempt %d/3)", 
						workerID, task.BlockID, backoff, retries+1)
					time.Sleep(backoff)
					continue
				}
				
				// 其他错误，不重试
				break
			}
			
			// 如果最终还是失败，记录错误
			if err != nil {
				log.Printf("Repair worker %d: Failed to update metadata for block %d (%s -> %s) after 3 retries: %v", 
					workerID, task.BlockID, task.ReplacedAddr, targetAddr, err)
			}
		}
	}
	
	log.Printf("Repair worker %d: Scheduled repair for block %d", workerID, task.BlockID)
}

// 重复函数已删除，使用前面的定义

// generateRepairTask 生成修复任务
func (ss *SchedulerService) generateRepairTask(blockID uint64, expectedLocations, actualLocations, missingLocations []string) *model.RepairTask {
	// 找一个健康的源位置
	var sourceAddr string
	for _, actual := range actualLocations {
		if ds := ss.clusterService.GetDataServerByAddr(actual); ds != nil {
			_, _, _, _, isHealthy := ds.GetStatus()
			if isHealthy && !ds.IsPermanentlyDownStatus() {
				sourceAddr = actual
				break
			}
		}
	}
	
	if sourceAddr == "" {
		log.Printf("No healthy source found for block %d", blockID)
		return nil
	}
	
	// 获取当前正在修复到的目标地址，避免重复调度
	repairingTargets := ss.getRepairingTargets(blockID)
	
	var targetAddrs []string
	var replacedAddr string
	var isReplacement bool
	
	for _, missingAddr := range missingLocations {
		// 检查是否已经在修复到这个地址
		alreadyRepairing := false
		for _, repairing := range repairingTargets {
			if missingAddr == repairing {
				alreadyRepairing = true
				break
			}
		}
		if alreadyRepairing {
			continue
		}
		
		// 检查原服务器是否健康
		if ds := ss.clusterService.GetDataServerByAddr(missingAddr); ds != nil {
			_, _, _, _, isHealthy := ds.GetStatus()
			if isHealthy && !ds.IsPermanentlyDownStatus() {
				// 原服务器健康，修复到原位置
				targetAddrs = append(targetAddrs, missingAddr)
				continue
			}
			
			// 检查是否永久宕机，需要替换
			if ds.IsPermanentlyDownStatus() {
				newTargetServer, err := ss.selectBestTargetServer(expectedLocations)
				if err != nil {
					log.Printf("Cannot find replacement server for block %d: %v", blockID, err)
					continue
				}
				
				targetAddrs = append(targetAddrs, newTargetServer.Addr)
				replacedAddr = missingAddr
				isReplacement = true
				continue
			}
		}
		
		// 原服务器不存在，选择新服务器
		newTargetServer, err := ss.selectBestTargetServer(expectedLocations)
		if err != nil {
			log.Printf("Cannot find target server for block %d: %v", blockID, err)
			continue
		}
		
		targetAddrs = append(targetAddrs, newTargetServer.Addr)
		replacedAddr = missingAddr
		isReplacement = true
	}
	
	if len(targetAddrs) == 0 {
		return nil
	}
	
	return &model.RepairTask{
		BlockID:           blockID,
		SourceAddr:        sourceAddr,
		TargetAddrs:       targetAddrs,
		IsReplacement:     isReplacement,
		ReplacedAddr:      replacedAddr,
		ExpectedLocations: expectedLocations,
		Priority:          1, // 默认高优先级
	}
}
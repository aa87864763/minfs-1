package handler

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	"metaServer/internal/service"
	"metaServer/pb"
)

type MetaServerHandler struct {
	pb.UnimplementedMetaServerServiceServer

	metadataService  *service.MetadataService
	clusterService   *service.ClusterService
	schedulerService *service.SchedulerService
	walService       *service.WALService
}

func NewMetaServerHandler(
	metadataService *service.MetadataService,
	clusterService *service.ClusterService,
	schedulerService *service.SchedulerService,
) *MetaServerHandler {
	return &MetaServerHandler{
		metadataService:  metadataService,
		clusterService:   clusterService,
		schedulerService: schedulerService,
		walService:       nil, // 将在main.go中设置
	}
}

// SetWALService 设置WAL服务
func (h *MetaServerHandler) SetWALService(walService *service.WALService) {
	h.walService = walService
}

// getWALService 获取WAL服务
func (h *MetaServerHandler) getWALService() *service.WALService {
	return h.walService
}

// isLeader 检查当前节点是否为leader
func (h *MetaServerHandler) isLeader() bool {
	if h.clusterService == nil {
		return false
	}
	return h.clusterService.IsLeader()
}

// createWALEntry 创建WAL条目
func (h *MetaServerHandler) createWALEntry(operation pb.WALOperationType, data interface{}) (*pb.LogEntry, error) {
	walService := h.getWALService()
	if walService == nil {
		// 如果没有WAL服务，跳过WAL记录
		return nil, nil
	}

	return walService.AppendLogEntry(operation, data)
}

// syncWALToFollowers 同步WAL到followers
func (h *MetaServerHandler) syncWALToFollowers(entry *pb.LogEntry) {
	walService := h.getWALService()
	if walService == nil {
		return
	}

	err := walService.SyncToFollowers(entry)
	if err != nil {
		log.Printf("Failed to sync WAL entry %d to followers: %v", entry.LogIndex, err)
	}
}

// CreateNode 创建文件或目录
func (h *MetaServerHandler) CreateNode(ctx context.Context, req *pb.CreateNodeRequest) (*pb.SimpleResponse, error) {
	log.Printf("CreateNode request: path=%s, type=%v", req.Path, req.Type)

	if req.Path == "" {
		return &pb.SimpleResponse{Success: false}, fmt.Errorf("path cannot be empty")
	}

	path := filepath.Clean(req.Path)
	if path == "." {
		path = "/"
	}

	// 检查是否为leader，只有leader才能处理写操作
	if !h.isLeader() {
		return &pb.SimpleResponse{Success: false}, fmt.Errorf("only leader can handle write operations")
	}

	// 1. 先执行实际的元数据操作，获得确定的inode ID
	err := h.metadataService.CreateNode(path, req.Type)
	if err != nil {
		log.Printf("CreateNode error: %v", err)
		return &pb.SimpleResponse{Success: false}, err
	}

	// 2. 获取创建后的文件信息（包含实际的inode ID）
	nodeInfo, err := h.metadataService.GetNodeInfo(path)
	if err != nil {
		log.Printf("CreateNode: Failed to get node info: %v", err)
		return &pb.SimpleResponse{Success: false}, err
	}

	// 3. 使用实际的inode ID创建WAL日志条目
	walEntry, err := h.createWALEntry(pb.WALOperationType_CREATE_NODE, &pb.CreateNodeOperation{
		Path:    path,
		Type:    req.Type,
		InodeId: nodeInfo.Inode, // 包含实际的inode ID
	})
	if err != nil {
		log.Printf("CreateNode: Failed to create WAL entry: %v", err)
		// 虽然WAL写入失败，但文件已创建，继续执行
	}

	// 3. 同步WAL到followers
	if walEntry != nil {
		go h.syncWALToFollowers(walEntry)
	}

	log.Printf("CreateNode success: %s", path)
	return &pb.SimpleResponse{Success: true}, nil
}

// GetNodeInfo 获取节点信息
func (h *MetaServerHandler) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	log.Printf("GetNodeInfo request: path=%s", req.Path)

	if req.Path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	nodeInfo, err := h.metadataService.GetNodeInfo(req.Path)
	if err != nil {
		log.Printf("GetNodeInfo error: %v", err)
		return nil, err
	}

	// 转换为 StatInfo 格式
	statInfo := h.nodeInfoToStatInfo(nodeInfo)

	return &pb.GetNodeInfoResponse{
		StatInfo: statInfo,
	}, nil
}

// ListDirectory 列出目录内容
func (h *MetaServerHandler) ListDirectory(ctx context.Context, req *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	log.Printf("ListDirectory request: path=%s", req.Path)

	if req.Path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	nodes, err := h.metadataService.ListDirectory(req.Path)
	if err != nil {
		log.Printf("ListDirectory error: %v", err)
		return nil, err
	}

	// 转换为 StatInfo 列表
	var statInfos []*pb.StatInfo
	for _, nodeInfo := range nodes {
		statInfos = append(statInfos, h.nodeInfoToStatInfo(nodeInfo))
	}

	log.Printf("ListDirectory success: %s (%d items)", req.Path, len(statInfos))
	return &pb.ListDirectoryResponse{
		Nodes: statInfos,
	}, nil
}

// DeleteNode 删除节点
func (h *MetaServerHandler) DeleteNode(ctx context.Context, req *pb.DeleteNodeRequest) (*pb.SimpleResponse, error) {
	log.Printf("DeleteNode request: path=%s, recursive=%v", req.Path, req.Recursive)

	if req.Path == "" {
		return &pb.SimpleResponse{Success: false}, fmt.Errorf("path cannot be empty")
	}

	if req.Path == "/" {
		return &pb.SimpleResponse{Success: false}, fmt.Errorf("cannot delete root directory")
	}

	// 检查Leader权限
	if !h.isLeader() {
		return &pb.SimpleResponse{Success: false}, fmt.Errorf("only leader can handle write operations")
	}

	// 1. 创建WAL日志条目
	walEntry, err := h.createWALEntry(pb.WALOperationType_DELETE_NODE, &pb.DeleteNodeOperation{
		Path:      req.Path,
		Recursive: req.Recursive,
	})
	if err != nil {
		log.Printf("DeleteNode: Failed to create WAL entry: %v", err)
		return &pb.SimpleResponse{Success: false}, err
	}

	// 2. 执行实际的元数据操作
	blocksToDelete, err := h.metadataService.DeleteNode(req.Path, req.Recursive)
	if err != nil {
		log.Printf("DeleteNode error: %v", err)
		return &pb.SimpleResponse{Success: false}, err
	}

	// 3. 同步WAL到followers
	if walEntry != nil {
		go h.syncWALToFollowers(walEntry)
	}

	// 4. 调度块删除
	for _, block := range blocksToDelete {
		h.schedulerService.ScheduleBlockDeletion(block.BlockID, block.Locations)
	}

	log.Printf("DeleteNode success: %s (%d blocks scheduled for deletion)", req.Path, len(blocksToDelete))
	return &pb.SimpleResponse{Success: true}, nil
}

// GetBlockLocations 获取数据块位置信息
func (h *MetaServerHandler) GetBlockLocations(ctx context.Context, req *pb.GetBlockLocationsRequest) (*pb.GetBlockLocationsResponse, error) {
	log.Printf("GetBlockLocations request: path=%s, size=%d", req.Path, req.Size)

	if req.Path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	path := filepath.Clean(req.Path)
	if path == "." {
		path = "/"
	}

	// 检查文件是否存在
	nodeInfo, err := h.metadataService.GetNodeInfo(path)
	if err != nil {
		// 文件不存在，创建新文件（写入模式）
		if req.Size > 0 {
			// 检查Leader权限
			if !h.isLeader() {
				return nil, fmt.Errorf("only leader can handle write operations")
			}

			// 1. 先执行实际的元数据操作，获得确定的inode ID
			err = h.metadataService.CreateNode(path, pb.FileType_File)
			if err != nil {
				return nil, err
			}

			// 2. 获取创建后的文件信息（包含实际的inode ID）
			nodeInfo, err = h.metadataService.GetNodeInfo(path)
			if err != nil {
				return nil, err
			}

			// 3. 使用实际的inode ID创建WAL日志条目
			walEntry, err := h.createWALEntry(pb.WALOperationType_CREATE_NODE, &pb.CreateNodeOperation{
				Path:    path,
				Type:    pb.FileType_File,
				InodeId: nodeInfo.Inode, // 包含实际的inode ID
			})
			if err != nil {
				log.Printf("GetBlockLocations: Failed to create WAL entry: %v", err)
				// 虽然WAL写入失败，但文件已创建，继续执行
			}

			// 4. 同步WAL到followers
			if walEntry != nil {
				go h.syncWALToFollowers(walEntry)
			}
		} else {
			return nil, fmt.Errorf("file not found: %s", path)
		}
	}

	if nodeInfo.Type == pb.FileType_Directory {
		return nil, fmt.Errorf("cannot get block locations for directory: %s", path)
	}

	// 判断是读取还是写入操作
	if req.Size == 0 || (req.Size == nodeInfo.Size && nodeInfo.Size > 0) {
		// 读取模式：返回已存在的块映射
		log.Printf("Read mode: getting existing blocks for %s", path)
		blockMappings, err := h.metadataService.GetBlockMappings(nodeInfo.Inode)
		if err != nil {
			return nil, fmt.Errorf("failed to get existing block mappings: %v", err)
		}

		log.Printf("GetBlockLocations (read) success: %s, inode=%d, %d existing blocks", path, nodeInfo.Inode, len(blockMappings))

		return &pb.GetBlockLocationsResponse{
			Inode:          nodeInfo.Inode,
			BlockLocations: blockMappings,
		}, nil
	} else {
		// 写入模式：分配新的数据块位置
		log.Printf("Write mode: allocating new blocks for %s", path)
		blockLocations, err := h.schedulerService.AllocateBlocks(uint64(req.Size), int(nodeInfo.Replication))
		if err != nil {
			return nil, err
		}

		// 保存块映射信息
		for i, blockLoc := range blockLocations {
			err = h.metadataService.SetBlockMapping(nodeInfo.Inode, uint64(i), blockLoc)
			if err != nil {
				return nil, err
			}
		}

		log.Printf("GetBlockLocations (write) success: %s, inode=%d, %d blocks allocated", path, nodeInfo.Inode, len(blockLocations))

		return &pb.GetBlockLocationsResponse{
			Inode:          nodeInfo.Inode,
			BlockLocations: blockLocations,
		}, nil
	}
}

// FinalizeWrite 完成文件写入
func (h *MetaServerHandler) FinalizeWrite(ctx context.Context, req *pb.FinalizeWriteRequest) (*pb.SimpleResponse, error) {
	log.Printf("FinalizeWrite request: path=%s, inode=%d, size=%d, md5=%s", req.Path, req.Inode, req.Size, req.Md5)

	if req.Path == "" {
		return &pb.SimpleResponse{Success: false}, fmt.Errorf("path cannot be empty")
	}

	if req.Inode == 0 {
		return &pb.SimpleResponse{Success: false}, fmt.Errorf("invalid inode ID")
	}

	// 只有leader才能执行写操作
	if !h.isLeader() {
		return &pb.SimpleResponse{Success: false}, fmt.Errorf("not leader, cannot execute FinalizeWrite")
	}

	// 获取块映射信息用于WAL记录
	blockMappings, err := h.metadataService.GetBlockMappings(req.Inode)
	if err != nil {
		log.Printf("FinalizeWrite error getting block mappings: %v", err)
		return &pb.SimpleResponse{Success: false}, err
	}

	// 记录到WAL
	walService := h.getWALService()
	if walService != nil {
		// 构建FinalizeWrite操作数据
		finalizeWriteOp := pb.FinalizeWriteOperation{
			Path:           req.Path,
			Inode:          req.Inode,
			Size:           req.Size,
			Md5:            req.Md5,
			BlockLocations: blockMappings,
		}

		// 添加WAL条目
		entry, err := walService.AppendLogEntry(pb.WALOperationType_FINALIZE_WRITE, finalizeWriteOp)
		if err != nil {
			log.Printf("FinalizeWrite failed to append WAL entry: %v", err)
			return &pb.SimpleResponse{Success: false}, err
		}

		// 同步到followers
		err = walService.SyncToFollowers(entry)
		if err != nil {
			log.Printf("FinalizeWrite failed to sync to followers: %v", err)
			// 注意：这里不返回错误，因为本地操作已成功，只是同步失败
		}
	}

	// 执行本地FinalizeWrite操作
	err = h.metadataService.FinalizeWrite(req.Path, req.Inode, uint64(req.Size), req.Md5)
	if err != nil {
		log.Printf("FinalizeWrite error: %v", err)
		return &pb.SimpleResponse{Success: false}, err
	}

	log.Printf("FinalizeWrite success: %s (MD5: %s)", req.Path, req.Md5)
	return &pb.SimpleResponse{Success: true}, nil
}

// GetClusterInfo 获取集群信息
func (h *MetaServerHandler) GetClusterInfo(ctx context.Context, req *pb.GetClusterInfoRequest) (*pb.GetClusterInfoResponse, error) {
	log.Printf("GetClusterInfo request")

	clusterInfo := h.clusterService.GetClusterInfo()

	log.Printf("GetClusterInfo success: %d dataServers", len(clusterInfo.DataServer))

	return &pb.GetClusterInfoResponse{
		ClusterInfo: clusterInfo,
	}, nil
}

// GetFileBlocks 获取已存在文件的块信息（用于读取）
func (h *MetaServerHandler) GetFileBlocks(ctx context.Context, req *pb.GetBlockLocationsRequest) (*pb.GetBlockLocationsResponse, error) {
	log.Printf("GetFileBlocks request: path=%s", req.Path)

	if req.Path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	path := filepath.Clean(req.Path)
	if path == "." {
		path = "/"
	}

	// 获取文件信息
	nodeInfo, err := h.metadataService.GetNodeInfo(path)
	if err != nil {
		return nil, fmt.Errorf("file not found: %v", err)
	}

	if nodeInfo.Type != pb.FileType_File {
		return nil, fmt.Errorf("path is not a file: %s", path)
	}

	// 获取已存在的块映射
	blockMappings, err := h.metadataService.GetBlockMappings(nodeInfo.Inode)
	if err != nil {
		return nil, fmt.Errorf("failed to get block mappings: %v", err)
	}

	log.Printf("GetFileBlocks success: %s, inode=%d, %d blocks found", path, nodeInfo.Inode, len(blockMappings))

	return &pb.GetBlockLocationsResponse{
		Inode:          nodeInfo.Inode,
		BlockLocations: blockMappings,
	}, nil
}

// Heartbeat 处理 DataServer 心跳
func (h *MetaServerHandler) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req.DataserverId == "" {
		return nil, fmt.Errorf("dataServer_id cannot be empty")
	}

	if req.DataserverAddr == "" {
		return nil, fmt.Errorf("dataServer_addr cannot be empty")
	}

	response, err := h.clusterService.ProcessHeartbeat(req)
	if err != nil {
		log.Printf("Heartbeat error from %s: %v", req.DataserverId, err)
		return nil, err
	}

	return response, nil
}

// GetReplicationInfo 获取文件副本分布信息
func (h *MetaServerHandler) GetReplicationInfo(ctx context.Context, req *pb.GetReplicationInfoRequest) (*pb.GetReplicationInfoResponse, error) {
	log.Printf("GetReplicationInfo request: path=%s", req.Path)

	var files []*pb.ReplicationStatus
	var totalFiles, healthyFiles, underReplicatedFiles, overReplicatedFiles uint32

	if req.Path != "" {
		// 查询特定文件的副本信息
		nodeInfo, err := h.metadataService.GetNodeInfo(req.Path)
		if err != nil {
			log.Printf("GetReplicationInfo error: file not found %s: %v", req.Path, err)
			return nil, fmt.Errorf("文件不存在: %s", req.Path)
		}

		if nodeInfo.Type != pb.FileType_File {
			return nil, fmt.Errorf("路径不是文件: %s", req.Path)
		}

		replicationStatus, err := h.buildReplicationStatus(nodeInfo)
		if err != nil {
			return nil, err
		}

		files = []*pb.ReplicationStatus{replicationStatus}
		totalFiles = 1

		switch replicationStatus.Status {
		case "healthy":
			healthyFiles = 1
		case "under_replicated":
			underReplicatedFiles = 1
		case "over_replicated":
			overReplicatedFiles = 1
		}
	} else {
		// 查询所有文件的副本信息 - 遍历所有文件
		allFiles, err := h.getAllFileNodes()
		if err != nil {
			return nil, fmt.Errorf("获取所有文件失败: %v", err)
		}

		for _, nodeInfo := range allFiles {
			replicationStatus, err := h.buildReplicationStatus(nodeInfo)
			if err != nil {
				log.Printf("GetReplicationInfo warning: failed to build status for %s: %v", nodeInfo.Path, err)
				continue
			}

			files = append(files, replicationStatus)
			totalFiles++

			switch replicationStatus.Status {
			case "healthy":
				healthyFiles++
			case "under_replicated":
				underReplicatedFiles++
			case "over_replicated":
				overReplicatedFiles++
			}
		}
	}

	log.Printf("GetReplicationInfo success: total=%d, healthy=%d, under=%d, over=%d",
		totalFiles, healthyFiles, underReplicatedFiles, overReplicatedFiles)

	return &pb.GetReplicationInfoResponse{
		Files:                files,
		TotalFiles:           totalFiles,
		HealthyFiles:         healthyFiles,
		UnderReplicatedFiles: underReplicatedFiles,
		OverReplicatedFiles:  overReplicatedFiles,
	}, nil
}

// buildReplicationStatus 构建单个文件的副本状态信息
func (h *MetaServerHandler) buildReplicationStatus(nodeInfo *pb.NodeInfo) (*pb.ReplicationStatus, error) {
	// 获取文件的所有块映射
	blockMappings, err := h.metadataService.GetBlockMappings(nodeInfo.Inode)
	if err != nil {
		return nil, fmt.Errorf("获取块映射失败: %v", err)
	}

	var blockInfos []*pb.BlockReplicationInfo
	var totalReplicas uint32 = 0
	var minReplicas uint32 = ^uint32(0) // 最大值

	for _, blockMapping := range blockMappings {
		actualLocations := make([]string, 0)

		// 检查每个声明的位置是否真实存在这个块
		for _, location := range blockMapping.Locations {
			dataServer := h.clusterService.GetDataServerByAddr(location)
			if dataServer != nil && dataServer.HasBlock(blockMapping.BlockId) {
				actualLocations = append(actualLocations, location)
			}
		}

		replicaCount := uint32(len(actualLocations))
		totalReplicas += replicaCount

		if replicaCount < minReplicas {
			minReplicas = replicaCount
		}

		blockInfo := &pb.BlockReplicationInfo{
			BlockId:           blockMapping.BlockId,
			Locations:         actualLocations,
			ExpectedLocations: blockMapping.Locations,
			ReplicaCount:      replicaCount,
		}

		blockInfos = append(blockInfos, blockInfo)
	}

	var actualReplicas uint32
	if len(blockMappings) > 0 {
		actualReplicas = totalReplicas / uint32(len(blockMappings))
	}

	// 确定健康状态
	status := "healthy"
	if minReplicas < nodeInfo.Replication {
		status = "under_replicated"
	} else if minReplicas > nodeInfo.Replication {
		status = "over_replicated"
	}

	return &pb.ReplicationStatus{
		Path:             nodeInfo.Path,
		Inode:            nodeInfo.Inode,
		Size:             nodeInfo.Size,
		ExpectedReplicas: nodeInfo.Replication,
		ActualReplicas:   actualReplicas,
		Blocks:           blockInfos,
		Status:           status,
	}, nil
}

// getAllFileNodes 获取所有文件节点信息
func (h *MetaServerHandler) getAllFileNodes() ([]*pb.NodeInfo, error) {
	var files []*pb.NodeInfo

	err := h.metadataService.TraverseAllFiles(func(nodeInfo *pb.NodeInfo) error {
		if nodeInfo.Type == pb.FileType_File {
			files = append(files, nodeInfo)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

// SyncWAL 同步 WAL（用于主从复制）
func (h *MetaServerHandler) SyncWAL(stream pb.MetaServerService_SyncWALServer) error {
	log.Printf("SyncWAL stream started")

	var processedCount int

	for {
		entry, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("SyncWAL stream ended, processed %d entries", processedCount)
				return stream.SendAndClose(&pb.SimpleResponse{
					Success: true,
					Message: fmt.Sprintf("Processed %d WAL entries", processedCount),
				})
			}
			log.Printf("SyncWAL stream error: %v", err)
			return err
		}

		// 检查是否有WAL服务
		walService := h.getWALService()
		if walService == nil {
			log.Printf("WAL service not available, skipping entry %d", entry.LogIndex)
			continue
		}

		// 检查当前节点是否为leader
		isLeader := h.isLeader()

		if isLeader {
			// 作为leader：回放WAL条目（执行操作）
			err = walService.ReplayLogEntry(entry, h.metadataService)
			if err != nil {
				log.Printf("Failed to replay WAL entry %d: %v", entry.LogIndex, err)
				return stream.SendAndClose(&pb.SimpleResponse{
					Success: false,
					Message: fmt.Sprintf("Failed to replay entry %d: %v", entry.LogIndex, err),
				})
			}
			log.Printf("SyncWAL (Leader): Replayed entry %d, operation: %v", entry.LogIndex, entry.Operation)
		} else {
			// 作为follower：只存储WAL条目，不执行操作
			log.Printf("SyncWAL (Follower): Storing entry %d without execution, operation: %v",
				entry.LogIndex, entry.Operation)
		}

		// 将日志条目写入本地WAL存储
		err = walService.WriteLogEntry(entry)
		if err != nil {
			log.Printf("Failed to write WAL entry %d locally: %v", entry.LogIndex, err)
			return stream.SendAndClose(&pb.SimpleResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to write entry %d locally: %v", entry.LogIndex, err),
			})
		}

		// 更新下一个日志索引
		walService.UpdateNextLogIndex(entry.LogIndex + 1)

		processedCount++
		log.Printf("SyncWAL: Processed entry %d, operation: %v, isLeader: %v",
			entry.LogIndex, entry.Operation, isLeader)
	}
}

// RequestWALSync 处理节点重连后的WAL同步请求
func (h *MetaServerHandler) RequestWALSync(req *pb.RequestWALSyncRequest, stream pb.MetaServerService_RequestWALSyncServer) error {
	log.Printf("RequestWALSync request from node %s, from index %d, reason: %s",
		req.NodeId, req.LastLogIndex, req.Reason)

	// 检查当前节点是否为leader
	if !h.isLeader() {
		return fmt.Errorf("only leader can handle WAL sync requests")
	}

	walService := h.getWALService()
	if walService == nil {
		return fmt.Errorf("WAL service not available")
	}

	// 获取从指定索引开始的所有WAL条目
	startIndex := req.LastLogIndex + 1
	if req.LastLogIndex == 0 {
		startIndex = 1 // 从第一个日志开始
	}

	entries, err := walService.GetAllLogEntries(startIndex)
	if err != nil {
		log.Printf("Failed to get WAL entries from index %d: %v", startIndex, err)
		return fmt.Errorf("failed to get WAL entries: %v", err)
	}

	log.Printf("Sending %d WAL entries to node %s starting from index %d",
		len(entries), req.NodeId, startIndex)

	// 发送WAL条目流
	sentCount := 0
	for _, entry := range entries {
		err = stream.Send(entry)
		if err != nil {
			log.Printf("Failed to send WAL entry %d to node %s: %v", entry.LogIndex, req.NodeId, err)
			return fmt.Errorf("failed to send WAL entry: %v", err)
		}
		sentCount++

		// 每发送100个条目记录一次进度
		if sentCount%100 == 0 {
			log.Printf("Sent %d/%d WAL entries to node %s", sentCount, len(entries), req.NodeId)
		}
	}

	log.Printf("WAL sync completed for node %s, sent %d entries", req.NodeId, sentCount)
	return nil
}

// GetLeader 获取主从信息 (HA 支持)
func (h *MetaServerHandler) GetLeader(ctx context.Context, req *pb.GetLeaderRequest) (*pb.GetLeaderResponse, error) {
	log.Printf("GetLeader request")

	// 通过clusterService获取选举信息
	clusterInfo := h.clusterService.GetClusterInfo()

	response := &pb.GetLeaderResponse{
		Leader:    clusterInfo.MasterMetaServer,
		Followers: clusterInfo.SlaveMetaServer,
	}

	// 安全地记录leader信息，避免空指针异常
	if response.Leader != nil {
		log.Printf("GetLeader response: Leader=%s:%d, Followers=%d",
			response.Leader.Host, response.Leader.Port, len(response.Followers))
	} else {
		log.Printf("GetLeader response: No leader available, Followers=%d", len(response.Followers))
	}

	return response, nil
}

// nodeInfoToStatInfo 将内部 NodeInfo 转换为 easyClient 需要的 StatInfo 格式
func (h *MetaServerHandler) nodeInfoToStatInfo(nodeInfo *pb.NodeInfo) *pb.StatInfo {
	// 获取副本数据
	replicaData := h.getReplicaDataForFile(nodeInfo.Path)

	return &pb.StatInfo{
		Path:        nodeInfo.Path,
		Size:        nodeInfo.Size,
		Mtime:       nodeInfo.Mtime,
		Type:        nodeInfo.Type, // 使用统一的 FileType
		ReplicaData: replicaData,
	}
}

// getReplicaDataForFile 获取文件的副本数据
func (h *MetaServerHandler) getReplicaDataForFile(path string) []*pb.ReplicaData {
	// TODO: 实现真正的副本数据获取
	// 这里先返回空列表，后续可以根据实际需求完善
	return []*pb.ReplicaData{}
}

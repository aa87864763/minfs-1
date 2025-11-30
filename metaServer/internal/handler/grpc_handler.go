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
	}
}

// isLeader 检查当前节点是否为leader
func (h *MetaServerHandler) isLeader() bool {
	if h.clusterService == nil {
		return false
	}
	return h.clusterService.IsLeader()
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

	// 执行元数据操作（内部会通过 Raft 同步）
	err := h.metadataService.CreateNode(path, req.Type)
	if err != nil {
		log.Printf("CreateNode error: %v", err)
		return &pb.SimpleResponse{Success: false}, err
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

	// DeleteNode 操作通过 Raft 自动同步
	blocksToDelete, err := h.metadataService.DeleteNode(req.Path, req.Recursive)
	if err != nil {
		log.Printf("DeleteNode error: %v", err)
		return &pb.SimpleResponse{Success: false}, err
	}

	// 调度块删除
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
			// CreateNode 操作通过 Raft 自动同步
			err = h.metadataService.CreateNode(path, pb.FileType_File)
			if err != nil {
				return nil, err
			}

			// 获取创建后的文件信息
			nodeInfo, err = h.metadataService.GetNodeInfo(path)
			if err != nil {
				return nil, err
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

	// FinalizeWrite 操作现在通过 Raft 自动同步
	// MetadataService.FinalizeWrite 内部会调用 RaftService.Apply
	err := h.metadataService.FinalizeWrite(req.Path, req.Inode, uint64(req.Size), req.Md5)
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
	if req.DataServerId == "" {
		return nil, fmt.Errorf("dataServer_id cannot be empty")
	}

	if req.DataServerAddr == "" {
		return nil, fmt.Errorf("dataServer_addr cannot be empty")
	}

	response, err := h.clusterService.ProcessHeartbeat(req)
	if err != nil {
		log.Printf("Heartbeat error from %s: %v", req.DataServerId, err)
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


// SyncWAL - [已废弃] 保留仅为兼容旧版 easyClient
// Raft 模式下调用会返回错误
func (h *MetaServerHandler) SyncWAL(stream pb.MetaServerService_SyncWALServer) error {
	log.Printf("[DEPRECATED] SyncWAL called - returning error (Raft mode)")
	return stream.SendAndClose(&pb.SimpleResponse{
		Success: false,
		Message: "SyncWAL is deprecated in Raft mode. Raft handles log replication automatically.",
	})
}

// RequestWALSync - [已废弃] 保留仅为兼容旧版 easyClient
// Raft 模式下调用会返回错误
func (h *MetaServerHandler) RequestWALSync(req *pb.RequestWALSyncRequest, stream pb.MetaServerService_RequestWALSyncServer) error {
	log.Printf("[DEPRECATED] RequestWALSync called from node %s - returning error (Raft mode)", req.NodeId)
	return fmt.Errorf("RequestWALSync is deprecated in Raft mode. Raft handles log replication automatically")
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

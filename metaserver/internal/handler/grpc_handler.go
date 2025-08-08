package handler

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	"metaserver/internal/service"
	"metaserver/pb"
)

type MetaServerHandler struct {
	pb.UnimplementedMetaServerServiceServer
	
	metadataService *service.MetadataService
	clusterService  *service.ClusterService
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
	
	return &pb.GetNodeInfoResponse{
		NodeInfo: nodeInfo,
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
	
	log.Printf("ListDirectory success: %s (%d items)", req.Path, len(nodes))
	return &pb.ListDirectoryResponse{
		Nodes: nodes,
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
			err = h.metadataService.CreateNode(path, pb.NodeType_FILE)
			if err != nil {
				return nil, err
			}
			
			nodeInfo, err = h.metadataService.GetNodeInfo(path)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("file not found: %s", path)
		}
	}
	
	if nodeInfo.Type == pb.NodeType_DIRECTORY {
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
		blockLocations, err := h.schedulerService.AllocateBlocks(req.Size, int(nodeInfo.Replication))
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
	
	err := h.metadataService.FinalizeWrite(req.Path, req.Inode, req.Size, req.Md5)
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
	
	dataservers := h.clusterService.GetClusterInfo()
	
	log.Printf("GetClusterInfo success: %d dataservers", len(dataservers))
	
	return &pb.GetClusterInfoResponse{
		Dataservers: dataservers,
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
	
	if nodeInfo.Type != pb.NodeType_FILE {
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
		return nil, fmt.Errorf("dataserver_id cannot be empty")
	}
	
	if req.DataserverAddr == "" {
		return nil, fmt.Errorf("dataserver_addr cannot be empty")
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
		
		if nodeInfo.Type != pb.NodeType_FILE {
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
		TotalFiles:          totalFiles,
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
		if nodeInfo.Type == pb.NodeType_FILE {
			files = append(files, nodeInfo)
		}
		return nil
	})
	
	if err != nil {
		return nil, err
	}
	
	return files, nil
}

// SyncWAL 同步 WAL（用于主从复制，暂未实现）
func (h *MetaServerHandler) SyncWAL(stream pb.MetaServerService_SyncWALServer) error {
	return fmt.Errorf("SyncWAL not implemented yet")
}
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
	for _, blockID := range blocksToDelete {
		h.schedulerService.ScheduleBlockDeletion(blockID, []string{})
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
	log.Printf("FinalizeWrite request: path=%s, inode=%d, size=%d", req.Path, req.Inode, req.Size)
	
	if req.Path == "" {
		return &pb.SimpleResponse{Success: false}, fmt.Errorf("path cannot be empty")
	}
	
	if req.Inode == 0 {
		return &pb.SimpleResponse{Success: false}, fmt.Errorf("invalid inode ID")
	}
	
	err := h.metadataService.FinalizeWrite(req.Path, req.Inode, req.Size)
	if err != nil {
		log.Printf("FinalizeWrite error: %v", err)
		return &pb.SimpleResponse{Success: false}, err
	}
	
	log.Printf("FinalizeWrite success: %s", req.Path)
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

// SyncWAL 同步 WAL（用于主从复制，暂未实现）
func (h *MetaServerHandler) SyncWAL(stream pb.MetaServerService_SyncWALServer) error {
	return fmt.Errorf("SyncWAL not implemented yet")
}
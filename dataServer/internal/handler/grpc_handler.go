package handler

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"dataServer/internal/model"
	"dataServer/pb"
)

// DataServerHandler 实现DataServerService的gRPC处理器
type DataServerHandler struct {
	pb.UnimplementedDataServerServiceServer

	storageService     model.StorageService
	replicationService model.ReplicationService
}

// NewDataServerHandler 创建新的DataServer处理器
func NewDataServerHandler(
	storageSvc model.StorageService,
	replicationSvc model.ReplicationService,
) *DataServerHandler {
	return &DataServerHandler{
		storageService:     storageSvc,
		replicationService: replicationSvc,
	}
}

// WriteBlock 实现流式写入数据块
func (h *DataServerHandler) WriteBlock(stream pb.DataServerService_WriteBlockServer) error {
	// 接收第一个消息（应该包含元数据）
	req, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive first message: %w", err)
	}

	// 验证第一个消息是否包含元数据
	if req.GetMetadata() == nil {
		return fmt.Errorf("first message must contain metadata")
	}

	metadata := req.GetMetadata()
	blockID := metadata.BlockId
	replicaLocations := metadata.ReplicaLocations

	log.Printf("Starting write block %d with %d replicas", blockID, len(replicaLocations))

	// 收集所有数据块
	var allData []byte

	// 继续接收数据块
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to receive data chunk: %w", err)
		}

		// 检查是否为数据块
		if chunkData := req.GetChunkData(); chunkData != nil {
			allData = append(allData, chunkData...)
		}
	}

	log.Printf("Received %d bytes for block %d", len(allData), blockID)

	// 主从同步复制：1个主库 + 1个同步从库 + 1个异步从库
	success := h.performMasterSlaveReplication(blockID, allData, replicaLocations)

	// 发送响应
	response := &pb.WriteBlockResponse{
		Success: success,
	}

	return stream.SendAndClose(response)
}

// ReadBlock 实现流式读取数据块
func (h *DataServerHandler) ReadBlock(req *pb.ReadBlockRequest, stream pb.DataServerService_ReadBlockServer) error {
	blockID := req.BlockId
	log.Printf("Reading block %d", blockID)

	// 从本地存储读取数据
	data, err := h.storageService.ReadBlock(blockID)
	if err != nil {
		log.Printf("Failed to read block %d: %v", blockID, err)
		return fmt.Errorf("failed to read block %d: %w", blockID, err)
	}

	log.Printf("Successfully read %d bytes for block %d", len(data), blockID)

	// 分块发送数据
	const chunkSize = 64 * 1024 // 64KB per chunk

	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := data[i:end]
		response := &pb.ReadBlockResponse{
			ChunkData: chunk,
		}

		if err := stream.Send(response); err != nil {
			log.Printf("Failed to send chunk for block %d: %v", blockID, err)
			return fmt.Errorf("failed to send data chunk: %w", err)
		}
	}

	log.Printf("Successfully sent all chunks for block %d", blockID)
	return nil
}

// DeleteBlock 实现删除数据块
func (h *DataServerHandler) DeleteBlock(ctx context.Context, req *pb.DeleteBlockRequest) (*pb.DeleteBlockResponse, error) {
	blockID := req.BlockId
	log.Printf("Deleting block %d", blockID)

	err := h.storageService.DeleteBlock(blockID)
	success := err == nil

	if err != nil {
		log.Printf("Failed to delete block %d: %v", blockID, err)
	} else {
		log.Printf("Successfully deleted block %d", blockID)
	}

	return &pb.DeleteBlockResponse{
		Success: success,
	}, nil
}

// CopyBlock 实现从源地址复制数据块
func (h *DataServerHandler) CopyBlock(ctx context.Context, req *pb.CopyBlockRequest) (*pb.CopyBlockResponse, error) {
	blockID := req.BlockId
	sourceAddr := req.SourceAddress

	log.Printf("Copying block %d from %s", blockID, sourceAddr)

	// 从源地址拉取数据
	data, err := h.replicationService.PullBlock(sourceAddr, blockID)
	if err != nil {
		log.Printf("Failed to pull block %d from %s: %v", blockID, sourceAddr, err)
		return &pb.CopyBlockResponse{
			Success: false,
		}, nil
	}

	// 存储到本地
	err = h.storageService.WriteBlock(blockID, data)
	success := err == nil

	if err != nil {
		log.Printf("Failed to store copied block %d: %v", blockID, err)
	} else {
		log.Printf("Successfully copied block %d from %s (%d bytes)", blockID, sourceAddr, len(data))
	}

	return &pb.CopyBlockResponse{
		Success: success,
	}, nil
}

// 实现简化的流接口

// writeBlockServer 实现WriteBlockStream接口
type writeBlockServer struct {
	stream pb.DataServerService_WriteBlockServer
}

func (s *writeBlockServer) Recv() (*pb.WriteBlockRequest, error) {
	return s.stream.Recv()
}

func (s *writeBlockServer) SendAndClose(resp *pb.WriteBlockResponse) error {
	return s.stream.SendAndClose(resp)
}

// readBlockServer 实现ReadBlockStream接口
type readBlockServer struct {
	stream pb.DataServerService_ReadBlockServer
}

func (s *readBlockServer) Send(resp *pb.ReadBlockResponse) error {
	return s.stream.Send(resp)
}

// 辅助函数：验证块ID
func (h *DataServerHandler) validateBlockID(blockID uint64) error {
	if blockID == 0 {
		return fmt.Errorf("invalid block ID: %d", blockID)
	}
	return nil
}

// performMasterSlaveReplication 执行主从同步复制
// 架构：1个主库 + 1个同步从库 + 1个异步从库
func (h *DataServerHandler) performMasterSlaveReplication(blockID uint64, data []byte, replicaLocations []string) bool {
	if len(replicaLocations) == 0 {
		log.Printf("No replica locations provided, only writing to local storage")
		return h.storageService.WriteBlock(blockID, data) == nil
	}

	// 步骤1：写入主库（当前dataServer）
	log.Printf("Step 1: Writing to master (local storage)")
	err := h.storageService.WriteBlock(blockID, data)
	if err != nil {
		log.Printf("Master write failed for block %d: %v", blockID, err)
		return false
	}
	log.Printf("Master write successful for block %d", blockID)

	// 步骤2：同步写入第一个从库
	if len(replicaLocations) >= 1 {
		syncSlaveAddr := replicaLocations[0]
		log.Printf("Step 2: Synchronous write to slave %s", syncSlaveAddr)

		err := h.replicationService.PushBlock(syncSlaveAddr, blockID, data)
		if err != nil {
			log.Printf("Synchronous slave write failed for block %d to %s: %v", blockID, syncSlaveAddr, err)

			// 回滚：删除主库中的数据
			log.Printf("Rolling back master write for block %d", blockID)
			if rollbackErr := h.storageService.DeleteBlock(blockID); rollbackErr != nil {
				log.Printf("Rollback failed for block %d: %v", blockID, rollbackErr)
			} else {
				log.Printf("Rollback successful for block %d", blockID)
			}
			return false
		}
		log.Printf("Synchronous slave write successful for block %d to %s", blockID, syncSlaveAddr)
	}

	// 步骤3：异步写入其他从库
	if len(replicaLocations) >= 2 {
		asyncSlaves := replicaLocations[1:]
		log.Printf("Step 3: Asynchronous write to %d slaves", len(asyncSlaves))

		// 使用goroutine异步写入
		go func() {
			var wg sync.WaitGroup
			for _, slaveAddr := range asyncSlaves {
				wg.Add(1)
				go func(addr string) {
					defer wg.Done()
					log.Printf("Async write to slave %s", addr)
					err := h.replicationService.PushBlock(addr, blockID, data)
					if err != nil {
						log.Printf("Async slave write failed for block %d to %s: %v", blockID, addr, err)
					} else {
						log.Printf("Async slave write successful for block %d to %s", blockID, addr)
					}
				}(slaveAddr)
			}
			wg.Wait()
			log.Printf("All async writes completed for block %d", blockID)
		}()
	}

	// 主库和同步从库都成功，返回true
	log.Printf("Master-slave replication successful for block %d", blockID)
	return true
}

// 辅助函数：记录操作统计
func (h *DataServerHandler) logOperationStats(operation string, blockID uint64, dataSize int, success bool) {
	status := "SUCCESS"
	if !success {
		status = "FAILED"
	}

	if dataSize > 0 {
		log.Printf("[STATS] %s block %d: %s (%d bytes)", operation, blockID, status, dataSize)
	} else {
		log.Printf("[STATS] %s block %d: %s", operation, blockID, status)
	}
}

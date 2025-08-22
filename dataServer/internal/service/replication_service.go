package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"dataServer/internal/model"
	"dataServer/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GrpcReplicationService gRPC复制服务实现
type GrpcReplicationService struct {
	connectionTimeout time.Duration
	connections       map[string]*grpc.ClientConn // 连接缓存
}

// NewReplicationService 创建新的复制服务实例
func NewReplicationService() *GrpcReplicationService {
	return &GrpcReplicationService{
		connectionTimeout: 5 * time.Second,
		connections:       make(map[string]*grpc.ClientConn),
	}
}

// ForwardBlock 作为gRPC客户端，将数据块转发到目标地址
func (s *GrpcReplicationService) ForwardBlock(targetAddr string, metadata *model.WriteBlockMetadata, data []byte) error {
	// 获取或创建连接
	conn, err := s.getConnection(targetAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", targetAddr, err)
	}

	// 创建客户端
	client := pb.NewDataServerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 开始流式传输
	stream, err := client.WriteBlock(ctx)
	if err != nil {
		return fmt.Errorf("failed to create write stream: %w", err)
	}

	// 发送元数据
	metadataReq := &pb.WriteBlockRequest{
		Content: &pb.WriteBlockRequest_Metadata{
			Metadata: &pb.WriteBlockMetadata{
				BlockId:          metadata.BlockId,
				ReplicaLocations: metadata.ReplicaLocations,
			},
		},
	}

	if err := stream.Send(metadataReq); err != nil {
		return fmt.Errorf("failed to send metadata: %w", err)
	}

	// 分块发送数据
	const chunkSize = 64 * 1024 // 64KB per chunk
	reader := bytes.NewReader(data)
	buffer := make([]byte, chunkSize)

	for {
		n, err := reader.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read data chunk: %w", err)
		}

		// 发送数据块
		chunkReq := &pb.WriteBlockRequest{
			Content: &pb.WriteBlockRequest_ChunkData{
				ChunkData: buffer[:n],
			},
		}

		if err := stream.Send(chunkReq); err != nil {
			return fmt.Errorf("failed to send data chunk: %w", err)
		}
	}

	// 关闭发送并接收响应
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close stream and receive response: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("remote server reported failure")
	}

	return nil
}

// PushBlock 推送数据块到目标地址
func (s *GrpcReplicationService) PushBlock(targetAddr string, blockID uint64, data []byte) error {
	// 创建简化的元数据（不需要副本位置，因为这是点对点传输）
	metadata := &model.WriteBlockMetadata{
		BlockId:          blockID,
		ReplicaLocations: []string{}, // 空副本列表，避免递归复制
	}

	// 直接使用ForwardBlock方法
	return s.ForwardBlock(targetAddr, metadata, data)
}

// PullBlock 从源地址拉取数据块并返回数据
func (s *GrpcReplicationService) PullBlock(sourceAddr string, blockID uint64) ([]byte, error) {
	// 获取或创建连接
	conn, err := s.getConnection(sourceAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", sourceAddr, err)
	}

	// 创建客户端
	client := pb.NewDataServerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 发起读取请求
	req := &pb.ReadBlockRequest{
		BlockId: blockID,
	}

	stream, err := client.ReadBlock(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create read stream: %w", err)
	}

	// 接收数据块
	var data []byte
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to receive data chunk: %w", err)
		}

		data = append(data, resp.ChunkData...)
	}

	return data, nil
}

// getConnection 获取或创建到目标地址的gRPC连接
func (s *GrpcReplicationService) getConnection(addr string) (*grpc.ClientConn, error) {
	// 检查是否已有连接
	if conn, exists := s.connections[addr]; exists {
		// 检查连接状态
		if conn.GetState().String() != "SHUTDOWN" {
			return conn, nil
		}
		// 连接已关闭，从缓存中删除
		delete(s.connections, addr)
	}

	// 创建新连接
	ctx, cancel := context.WithTimeout(context.Background(), s.connectionTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // 等待连接建立
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	// 缓存连接
	s.connections[addr] = conn

	return conn, nil
}

// Close 关闭所有连接
func (s *GrpcReplicationService) Close() error {
	for addr, conn := range s.connections {
		if err := conn.Close(); err != nil {
			// 记录错误但继续关闭其他连接
			fmt.Printf("Failed to close connection to %s: %v\n", addr, err)
		}
	}

	// 清空连接缓存
	s.connections = make(map[string]*grpc.ClientConn)

	return nil
}

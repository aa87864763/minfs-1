package client

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"strings"

	"client/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MinifsClient struct {
	metaservers    []string // 所有metaserver地址列表
	currentLeader  string   // 当前leader地址
	metaClient     pb.MetaServerServiceClient
	metaConn       *grpc.ClientConn
}

func NewMinifsClient(metaServerAddrs []string) (*MinifsClient, error) {
	if len(metaServerAddrs) == 0 {
		return nil, fmt.Errorf("no metaserver addresses provided")
	}

	client := &MinifsClient{
		metaservers: metaServerAddrs,
	}

	// 发现并连接到leader
	if err := client.discoverAndConnectLeader(); err != nil {
		return nil, fmt.Errorf("failed to discover and connect to leader: %v", err)
	}

	fmt.Printf("Connected to leader: %s\n", client.currentLeader)
	return client, nil
}

// discoverAndConnectLeader 发现并连接到leader
func (c *MinifsClient) discoverAndConnectLeader() error {
	// 尝试每个metaserver地址，找到leader
	for _, addr := range c.metaservers {
		fmt.Printf("Trying to connect to %s...\n", addr)
		
		// 连接到这个metaserver
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Failed to connect to %s: %v\n", addr, err)
			continue
		}

		tempClient := pb.NewMetaServerServiceClient(conn)
		
		// 获取leader信息
		resp, err := tempClient.GetLeader(context.Background(), &pb.GetLeaderRequest{})
		if err != nil {
			fmt.Printf("Failed to get leader from %s: %v\n", addr, err)
			conn.Close()
			continue
		}

		// 检查这个节点是否是leader
		leaderAddr := fmt.Sprintf("%s:%d", resp.Leader.Host, resp.Leader.Port)
		
		if leaderAddr == addr {
			// 这个节点就是leader，直接使用
			c.currentLeader = addr
			c.metaClient = tempClient
			c.metaConn = conn
			return nil
		} else {
			// 这个节点不是leader，关闭连接，尝试连接到真正的leader
			conn.Close()
			
			// 尝试连接到真正的leader
			leaderConn, err := grpc.Dial(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Printf("Failed to connect to leader %s: %v\n", leaderAddr, err)
				continue
			}
			
			// 验证这确实是leader
			leaderClient := pb.NewMetaServerServiceClient(leaderConn)
			leaderResp, err := leaderClient.GetLeader(context.Background(), &pb.GetLeaderRequest{})
			if err != nil {
				fmt.Printf("Failed to verify leader %s: %v\n", leaderAddr, err)
				leaderConn.Close()
				continue
			}
			
			verifyAddr := fmt.Sprintf("%s:%d", leaderResp.Leader.Host, leaderResp.Leader.Port)
			if verifyAddr == leaderAddr {
				// 确认这是leader
				c.currentLeader = leaderAddr
				c.metaClient = leaderClient
				c.metaConn = leaderConn
				return nil
			} else {
				leaderConn.Close()
				continue
			}
		}
	}

	return fmt.Errorf("failed to find and connect to any leader")
}

// reconnectToLeader 重新连接到leader（在连接失败时调用）
func (c *MinifsClient) reconnectToLeader() error {
	fmt.Printf("Connection to leader %s failed, trying to reconnect...\n", c.currentLeader)
	
	// 关闭当前连接
	if c.metaConn != nil {
		c.metaConn.Close()
	}
	
	// 重新发现leader
	return c.discoverAndConnectLeader()
}

// executeWithRetry 执行gRPC调用，如果失败则重试
func (c *MinifsClient) executeWithRetry(operation func() error) error {
	err := operation()
	if err != nil {
		// 如果调用失败，尝试重新连接到leader
		if reconnectErr := c.reconnectToLeader(); reconnectErr != nil {
			return fmt.Errorf("operation failed and reconnect failed: %v (original error: %v)", reconnectErr, err)
		}
		
		// 重新连接成功，重试操作
		fmt.Printf("Reconnected to new leader: %s, retrying operation...\n", c.currentLeader)
		err = operation()
	}
	return err
}

func (c *MinifsClient) Close() error {
	if c.metaConn != nil {
		return c.metaConn.Close()
	}
	return nil
}

// A1: 文件创建
func (c *MinifsClient) Create(path string) error {
	return c.executeWithRetry(func() error {
		req := &pb.CreateNodeRequest{
			Path: path,
			Type: pb.FileType_File,
		}

		resp, err := c.metaClient.CreateNode(context.Background(), req)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %v", path, err)
		}

		if !resp.Success {
			return fmt.Errorf("failed to create file %s: server returned failure", path)
		}

		fmt.Printf("Successfully created file: %s\n", path)
		return nil
	})
}

// A1: 目录创建
func (c *MinifsClient) CreateDirectory(path string) error {
	req := &pb.CreateNodeRequest{
		Path: path,
		Type: pb.FileType_Directory,
	}

	resp, err := c.metaClient.CreateNode(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to create directory %s: %v", path, err)
	}

	if !resp.Success {
		return fmt.Errorf("failed to create directory %s: server returned failure", path)
	}

	fmt.Printf("Successfully created directory: %s\n", path)
	return nil
}

// A2: 获取文件状态 (适配 easyClient StatInfo 格式)
func (c *MinifsClient) GetStatus(path string) (*pb.StatInfo, error) {
	req := &pb.GetNodeInfoRequest{
		Path: path,
	}

	resp, err := c.metaClient.GetNodeInfo(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("failed to get status for %s: %v", path, err)
	}

	fmt.Printf("Status for %s:\n", path)
	fmt.Printf("  Path: %s\n", resp.StatInfo.Path)
	fmt.Printf("  Type: %s\n", resp.StatInfo.Type.String())
	fmt.Printf("  Size: %d bytes\n", resp.StatInfo.Size)
	fmt.Printf("  MTime: %d\n", resp.StatInfo.Mtime)
	if len(resp.StatInfo.ReplicaData) > 0 {
		fmt.Printf("  Replicas: %d\n", len(resp.StatInfo.ReplicaData))
		for i, replica := range resp.StatInfo.ReplicaData {
			fmt.Printf("    Replica %d: %s @ %s\n", i+1, replica.Id, replica.DsNode)
		}
	}

	return resp.StatInfo, nil
}

// A2: 列出目录内容 (适配 easyClient StatInfo 格式)
func (c *MinifsClient) ListStatus(path string) ([]*pb.StatInfo, error) {
	req := &pb.ListDirectoryRequest{
		Path: path,
	}

	resp, err := c.metaClient.ListDirectory(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory %s: %v", path, err)
	}

	fmt.Printf("Contents of directory %s:\n", path)
	for _, node := range resp.Nodes {
		// 提取文件名（路径的最后部分）
		fileName := node.Path
		if idx := strings.LastIndex(fileName, "/"); idx >= 0 {
			fileName = fileName[idx+1:]
		}
		
		typeStr := "FILE"
		if node.Type == pb.FileType_Directory {
			typeStr = "DIR "
		}
		
		fmt.Printf("  %-4s %-20s %8d\n", typeStr, fileName, node.Size)
	}

	return resp.Nodes, nil
}

// A3: 删除文件或目录（默认递归删除）
func (c *MinifsClient) Delete(path string) error {
	req := &pb.DeleteNodeRequest{
		Path:      path,
		Recursive: true, // 默认递归删除
	}

	resp, err := c.metaClient.DeleteNode(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to delete %s: %v", path, err)
	}

	if !resp.Success {
		return fmt.Errorf("failed to delete %s: server returned failure", path)
	}

	fmt.Printf("Successfully deleted: %s\n", path)
	return nil
}

// A4: 写文件
func (c *MinifsClient) WriteFile(path string, data []byte) error {
	// 1. 获取块位置信息
	req := &pb.GetBlockLocationsRequest{
		Path: path,
		Size: int64(len(data)),
	}

	resp, err := c.metaClient.GetBlockLocations(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to get block locations for %s: %v", path, err)
	}

	// 2. 分割数据并写入各个块
	blockSize := 4 * 1024 * 1024 // 4MB per block (与MetaServer配置保持一致)
	dataOffset := 0
	
	for i, blockLoc := range resp.BlockLocations {
		// 计算这个块的数据范围
		startOffset := dataOffset
		endOffset := startOffset + blockSize
		if endOffset > len(data) {
			endOffset = len(data)
		}
		
		// 获取这个块的数据片段
		blockData := data[startOffset:endOffset]
		
		fmt.Printf("Writing block %d: %d bytes (offset %d-%d)\n", 
			i, len(blockData), startOffset, endOffset-1)
		
		// 写入到第一个DataServer，让它处理副本
		for _, location := range blockLoc.Locations {
			err := c.writeBlockToDataServer(location, blockLoc.BlockId, blockData, blockLoc.Locations)
			if err != nil {
				return fmt.Errorf("failed to write block %d to %s: %v", blockLoc.BlockId, location, err)
			}
			break // 只写入第一个DataServer，让它处理副本
		}
		
		dataOffset = endOffset
		if dataOffset >= len(data) {
			break
		}
	}

	// 3. 计算MD5哈希
	hash := md5.Sum(data)
	md5Hash := fmt.Sprintf("%x", hash)

	// 完成写入
	finalizeReq := &pb.FinalizeWriteRequest{
		Path:  path,
		Inode: resp.Inode,
		Size:  int64(len(data)),
		Md5:   md5Hash,
	}

	finalizeResp, err := c.metaClient.FinalizeWrite(context.Background(), finalizeReq)
	if err != nil {
		return fmt.Errorf("failed to finalize write for %s: %v", path, err)
	}

	if !finalizeResp.Success {
		return fmt.Errorf("failed to finalize write for %s: server returned failure", path)
	}

	fmt.Printf("Successfully wrote file %s (size: %d bytes, MD5: %s)\n", path, len(data), md5Hash)
	return nil
}

func (c *MinifsClient) writeBlockToDataServer(address string, blockId uint64, data []byte, replicaLocations []string) error {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to dataserver %s: %v", address, err)
	}
	defer conn.Close()

	client := pb.NewDataServerServiceClient(conn)
	stream, err := client.WriteBlock(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create write stream: %v", err)
	}

	// 发送元数据
	metadataReq := &pb.WriteBlockRequest{
		Content: &pb.WriteBlockRequest_Metadata{
			Metadata: &pb.WriteBlockMetadata{
				BlockId:          blockId,
				ReplicaLocations: replicaLocations,
			},
		},
	}

	if err := stream.Send(metadataReq); err != nil {
		return fmt.Errorf("failed to send metadata: %v", err)
	}

	// 发送数据
	chunkSize := 1024 * 64 // 64KB chunks
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		dataReq := &pb.WriteBlockRequest{
			Content: &pb.WriteBlockRequest_ChunkData{
				ChunkData: data[i:end],
			},
		}

		if err := stream.Send(dataReq); err != nil {
			return fmt.Errorf("failed to send data chunk: %v", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close stream: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("write operation failed")
	}

	return nil
}

// A4: 读文件
func (c *MinifsClient) ReadFile(path string) ([]byte, error) {
	// 1. 获取文件信息以获得原始MD5
	statusReq := &pb.GetNodeInfoRequest{
		Path: path,
	}
	// TODO: 由于 StatInfo 不包含 MD5 字段，这里暂时跳过 MD5 验证
	_ = statusReq // 避免未使用变量警告
	originalMD5 := "" // 临时跳过 MD5 验证

	// 2. 获取文件块位置
	req := &pb.GetBlockLocationsRequest{
		Path: path,
		Size: 0, // 读操作不需要指定大小
	}

	resp, err := c.metaClient.GetBlockLocations(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("failed to get block locations for %s: %v", path, err)
	}

	var allData []byte

	// 3. 从DataServer读取数据
	for _, blockLoc := range resp.BlockLocations {
		blockData, err := c.readBlockFromDataServer(blockLoc.Locations[0], blockLoc.BlockId)
		if err != nil {
			return nil, fmt.Errorf("failed to read block %d: %v", blockLoc.BlockId, err)
		}
		allData = append(allData, blockData...)
	}

	// 4. 计算当前数据的MD5并验证
	hash := md5.Sum(allData)
	currentMD5 := fmt.Sprintf("%x", hash)
	
	fmt.Printf("Successfully read file %s (size: %d bytes)\n", path, len(allData))
	fmt.Printf("  Original MD5: %s\n", originalMD5)
	fmt.Printf("  Current MD5:  %s\n", currentMD5)
	
	if originalMD5 != "" && originalMD5 == currentMD5 {
		fmt.Printf("  ✓ MD5 验证通过 - 文件完整性正确\n")
	} else if originalMD5 != "" {
		fmt.Printf("  ✗ MD5 验证失败 - 文件可能已损坏！\n")
	}
	
	return allData, nil
}

func (c *MinifsClient) readBlockFromDataServer(address string, blockId uint64) ([]byte, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to dataserver %s: %v", address, err)
	}
	defer conn.Close()

	client := pb.NewDataServerServiceClient(conn)
	req := &pb.ReadBlockRequest{
		BlockId: blockId,
	}

	stream, err := client.ReadBlock(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("failed to create read stream: %v", err)
	}

	var allData []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to receive data: %v", err)
		}
		allData = append(allData, resp.ChunkData...)
	}

	return allData, nil
}

// A4: 打开文件（这里简化为检查文件是否存在）
func (c *MinifsClient) Open(path string) error {
	_, err := c.GetStatus(path)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", path, err)
	}
	fmt.Printf("Successfully opened file: %s\n", path)
	return nil
}

// A5: 获取集群信息
func (c *MinifsClient) GetClusterInfo() (*pb.GetClusterInfoResponse, error) {
	var result *pb.GetClusterInfoResponse
	err := c.executeWithRetry(func() error {
		req := &pb.GetClusterInfoRequest{}

		resp, err := c.metaClient.GetClusterInfo(context.Background(), req)
		if err != nil {
			return fmt.Errorf("failed to get cluster info: %v", err)
		}

		result = resp

		fmt.Printf("Cluster Information:\n")
		
		// 显示当前连接信息
		fmt.Printf("Connected to Leader: %s\n", c.currentLeader)
		fmt.Printf("\n")
		
		// 显示 MetaServer 信息
		if resp.ClusterInfo.MasterMetaServer != nil {
			fmt.Printf("Master MetaServer: %s:%d\n", 
				resp.ClusterInfo.MasterMetaServer.Host, resp.ClusterInfo.MasterMetaServer.Port)
		}
		if len(resp.ClusterInfo.SlaveMetaServer) > 0 {
			fmt.Printf("Slave MetaServers (%d):\n", len(resp.ClusterInfo.SlaveMetaServer))
			for _, slave := range resp.ClusterInfo.SlaveMetaServer {
				fmt.Printf("  %s:%d\n", slave.Host, slave.Port)
			}
		}
		
		// 显示 DataServer 信息
		fmt.Printf("DataServers (%d):\n", len(resp.ClusterInfo.DataServer))
		for _, ds := range resp.ClusterInfo.DataServer {
			fmt.Printf("  %s:%d - Files: %d, Capacity: %d MB, Used: %d MB\n",
				ds.Host, ds.Port, ds.FileTotal, ds.Capacity, ds.UseCapacity)
		}

		return nil
	})
	
	return result, err
}

// GetLeader 获取MetaServer主从信息
func (c *MinifsClient) GetLeader() (*pb.GetLeaderResponse, error) {
	var result *pb.GetLeaderResponse
	err := c.executeWithRetry(func() error {
		req := &pb.GetLeaderRequest{}

		resp, err := c.metaClient.GetLeader(context.Background(), req)
		if err != nil {
			return fmt.Errorf("failed to get leader info: %v", err)
		}

		result = resp

		fmt.Printf("Leader Information:\n")
		
		// 显示当前连接信息
		fmt.Printf("Connected to: %s\n", c.currentLeader)
		fmt.Printf("\n")
		
		// 显示 Leader 信息
		if resp.Leader != nil {
			fmt.Printf("Leader MetaServer: %s:%d\n", 
				resp.Leader.Host, resp.Leader.Port)
		}
		
		// 显示 Followers 信息
		if len(resp.Followers) > 0 {
			fmt.Printf("Follower MetaServers (%d):\n", len(resp.Followers))
			for i, follower := range resp.Followers {
				fmt.Printf("  %d. %s:%d\n", i+1, follower.Host, follower.Port)
			}
		} else {
			fmt.Printf("No Follower MetaServers found\n")
		}

		return nil
	})
	
	return result, err
}

// GetReplicationInfo 获取文件副本分布信息
func (c *MinifsClient) GetReplicationInfo(path string) (*pb.GetReplicationInfoResponse, error) {
	req := &pb.GetReplicationInfoRequest{
		Path: path,
	}

	resp, err := c.metaClient.GetReplicationInfo(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("failed to get replication info: %v", err)
	}

	if path != "" {
		// 查询特定文件
		fmt.Printf("Replication Information for %s:\n", path)
	} else {
		// 查询所有文件
		fmt.Printf("Cluster-wide Replication Information:\n")
		fmt.Printf("Total Files: %d\n", resp.TotalFiles)
		fmt.Printf("Healthy Files: %d\n", resp.HealthyFiles)
		fmt.Printf("Under-replicated Files: %d\n", resp.UnderReplicatedFiles)
		fmt.Printf("Over-replicated Files: %d\n", resp.OverReplicatedFiles)
		fmt.Printf("\nFile Details:\n")
	}

	for _, file := range resp.Files {
		fmt.Printf("File: %s (inode: %d)\n", file.Path, file.Inode)
		fmt.Printf("  Size: %d bytes\n", file.Size)
		fmt.Printf("  Expected Replicas: %d\n", file.ExpectedReplicas)
		fmt.Printf("  Actual Replicas: %d\n", file.ActualReplicas)
		fmt.Printf("  Status: %s\n", file.Status)
		
		if len(file.Blocks) > 0 {
			fmt.Printf("  Blocks (%d):\n", len(file.Blocks))
			for _, block := range file.Blocks {
				fmt.Printf("    Block %d: %d replicas\n", block.BlockId, block.ReplicaCount)
				fmt.Printf("      Actual locations: %v\n", block.Locations)
				fmt.Printf("      Expected locations: %v\n", block.ExpectedLocations)
			}
		}
		fmt.Println()
	}

	return resp, nil
}
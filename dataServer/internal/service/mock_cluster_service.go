package service

import (
	"fmt"
	"log"
	"time"

	"dataServer/internal/model"
)

// MockClusterService 模拟集群服务，用于测试时替代真实的etcd服务
type MockClusterService struct {
	config         *model.Config
	storageService model.StorageService
	isRunning      bool
	stopChan       chan struct{}
}

// NewMockClusterService 创建模拟集群服务
func NewMockClusterService(config *model.Config, storageService model.StorageService) *MockClusterService {
	return &MockClusterService{
		config:         config,
		storageService: storageService,
		stopChan:       make(chan struct{}),
	}
}

// RegisterToETCD 模拟注册到etcd
func (s *MockClusterService) RegisterToETCD() error {
	key := fmt.Sprintf("/minfs/dataServer/%s", s.config.Server.DataserverId)
	value := s.config.Server.ListenAddress

	log.Printf("[MOCK] Registered to etcd: %s -> %s", key, value)
	log.Println("[MOCK] Service registration successful (simulated)")

	return nil
}

// StartHeartbeatLoop 模拟启动心跳循环
func (s *MockClusterService) StartHeartbeatLoop() error {
	if s.isRunning {
		return fmt.Errorf("heartbeat loop is already running")
	}

	s.isRunning = true

	// 启动模拟心跳
	go s.mockHeartbeatLoop()

	log.Println("[MOCK] Heartbeat loop started (simulated)")
	return nil
}

// Stop 停止模拟集群服务
func (s *MockClusterService) Stop() error {
	if !s.isRunning {
		return nil
	}

	close(s.stopChan)
	s.isRunning = false

	log.Println("[MOCK] Cluster service stopped")
	return nil
}

// mockHeartbeatLoop 模拟心跳循环
func (s *MockClusterService) mockHeartbeatLoop() {
	ticker := time.NewTicker(time.Duration(s.config.MetaServer.HeartbeatInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sendMockHeartbeat()

		case <-s.stopChan:
			log.Println("[MOCK] Heartbeat loop stopping")
			return
		}
	}
}

// sendMockHeartbeat 发送模拟心跳
func (s *MockClusterService) sendMockHeartbeat() {
	// 获取存储统计
	stat, err := s.storageService.GetStat()
	if err != nil {
		log.Printf("[MOCK] Failed to get storage stat: %v", err)
		return
	}

	// 打印详细的心跳数据到控制台
	log.Printf("📡 [MOCK HEARTBEAT REQUEST] DataServer: %s", s.config.Server.DataserverId)
	log.Printf("    └── Address: %s", s.config.Server.ListenAddress)
	log.Printf("    └── Block Count: %d", stat.BlockCount)
	log.Printf("    └── Free Space: %d bytes (%.2f MB)", stat.FreeSpace, float64(stat.FreeSpace)/(1024*1024))
	log.Printf("    └── Used Space: %d bytes (%.2f MB)", stat.UsedSpace, float64(stat.UsedSpace)/(1024*1024))
	if len(stat.BlockIds) > 0 {
		if len(stat.BlockIds) <= 10 {
			log.Printf("    └── Block IDs: %v", stat.BlockIds)
		} else {
			log.Printf("    └── Block IDs: %v... (total: %d blocks)", stat.BlockIds[:10], len(stat.BlockIds))
		}
	} else {
		log.Printf("    └── Block IDs: [] (no blocks stored)")
	}

	log.Printf("💓 [MOCK HEARTBEAT RESPONSE] No commands from MetaServer (simulated)")

	// 模拟处理一些命令（用于测试）
	s.simulateCommands()
}

// simulateCommands 模拟处理一些命令
func (s *MockClusterService) simulateCommands() {
	// 这里可以模拟一些测试场景
	// 例如：每隔一段时间模拟删除某些块、复制某些块等

	// 目前为空实现，可根据测试需要添加逻辑
}

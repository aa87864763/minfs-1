package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"metaServer/internal/model"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdService 提供 etcd 服务注册和发现功能
type EtcdService struct {
	client   *clientv3.Client
	config   *model.Config
	
	// 用于监听 DataServer 变化的回调函数
	onDataServerChange func(string, *DataServerRegistration, bool) // id, data, isOnline
	
	// 停止监听的通道
	stopChan chan bool
}

// DataServerRegistration DataServer 在 etcd 中的注册信息
type DataServerRegistration struct {
	ID         string    `json:"id"`
	Addr       string    `json:"addr"`
	RegisterAt time.Time `json:"register_at"`
}

// NewEtcdService 创建新的 etcd 服务
func NewEtcdService(config *model.Config) (*EtcdService, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Etcd.Endpoints,
		DialTimeout: config.Etcd.Timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %v", err)
	}

	return &EtcdService{
		client:   client,
		config:   config,
		stopChan: make(chan bool),
	}, nil
}

// StartWatching 开始监听 DataServer 的注册/注销事件
func (es *EtcdService) StartWatching(onDataServerChange func(string, *DataServerRegistration, bool)) error {
	es.onDataServerChange = onDataServerChange

	// 首先获取当前已存在的 DataServer
	ctx, cancel := context.WithTimeout(context.Background(), es.config.Etcd.Timeout)
	defer cancel()

	resp, err := es.client.Get(ctx, es.config.Etcd.DataServerKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get existing DataServers: %v", err)
	}

	// 处理已存在的 DataServer
	for _, kv := range resp.Kvs {
		var registration DataServerRegistration
		if err := json.Unmarshal(kv.Value, &registration); err != nil {
			log.Printf("Failed to unmarshal DataServer registration: %v", err)
			continue
		}

		log.Printf("Found existing DataServer: %s at %s", registration.ID, registration.Addr)
		es.onDataServerChange(registration.ID, &registration, true)
	}

	// 启动监听协程
	go es.watchDataServers()

	log.Printf("Started watching DataServers on etcd with prefix: %s", es.config.Etcd.DataServerKeyPrefix)
	return nil
}

// watchDataServers 监听 DataServer 的变化
func (es *EtcdService) watchDataServers() {
	watchChan := es.client.Watch(context.Background(), es.config.Etcd.DataServerKeyPrefix, clientv3.WithPrefix())

	for {
		select {
		case watchResp := <-watchChan:
			for _, event := range watchResp.Events {
				key := string(event.Kv.Key)
				
				// 从 key 中提取 DataServer ID
				dsID := es.extractDataServerID(key)
				if dsID == "" {
					continue
				}

				switch event.Type {
				case clientv3.EventTypePut:
					// DataServer 注册或更新
					var registration DataServerRegistration
					if err := json.Unmarshal(event.Kv.Value, &registration); err != nil {
						log.Printf("Failed to unmarshal DataServer registration: %v", err)
						continue
					}

					log.Printf("DataServer registered/updated: %s at %s", registration.ID, registration.Addr)
					es.onDataServerChange(registration.ID, &registration, true)

				case clientv3.EventTypeDelete:
					// DataServer 注销
					log.Printf("DataServer unregistered: %s", dsID)
					es.onDataServerChange(dsID, nil, false)
				}
			}

		case <-es.stopChan:
			log.Println("Stopped watching DataServers")
			return
		}
	}
}

// extractDataServerID 从 etcd key 中提取 DataServer ID
func (es *EtcdService) extractDataServerID(key string) string {
	prefix := es.config.Etcd.DataServerKeyPrefix
	if len(key) > len(prefix) {
		return key[len(prefix):]
	}
	return ""
}

// RegisterMetaServer 将 MetaServer 注册到 etcd（用于高可用模式）
func (es *EtcdService) RegisterMetaServer(metaServerID, addr string) error {
	registration := map[string]interface{}{
		"id":          metaServerID,
		"addr":        addr,
		"register_at": time.Now(),
		"role":        "metaServer",
	}

	data, err := json.Marshal(registration)
	if err != nil {
		return fmt.Errorf("failed to marshal MetaServer registration: %v", err)
	}

	key := fmt.Sprintf("/minfs/metaServers/%s", metaServerID)
	
	ctx, cancel := context.WithTimeout(context.Background(), es.config.Etcd.Timeout)
	defer cancel()

	_, err = es.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to register MetaServer: %v", err)
	}

	log.Printf("MetaServer registered in etcd: %s at %s", metaServerID, addr)
	return nil
}

// GetAllDataServers 从 etcd 获取所有 DataServer 信息
func (es *EtcdService) GetAllDataServers() (map[string]*DataServerRegistration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), es.config.Etcd.Timeout)
	defer cancel()

	resp, err := es.client.Get(ctx, es.config.Etcd.DataServerKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get DataServers from etcd: %v", err)
	}

	dataServers := make(map[string]*DataServerRegistration)
	for _, kv := range resp.Kvs {
		var registration DataServerRegistration
		if err := json.Unmarshal(kv.Value, &registration); err != nil {
			log.Printf("Failed to unmarshal DataServer registration: %v", err)
			continue
		}

		dataServers[registration.ID] = &registration
	}

	return dataServers, nil
}

// DeleteDataServer 从 etcd 中删除 DataServer 注册信息
func (es *EtcdService) DeleteDataServer(dataServerID string) error {
	key := es.config.Etcd.DataServerKeyPrefix + dataServerID
	
	ctx, cancel := context.WithTimeout(context.Background(), es.config.Etcd.Timeout)
	defer cancel()

	_, err := es.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete DataServer from etcd: %v", err)
	}

	log.Printf("Deleted DataServer from etcd: %s", dataServerID)
	return nil
}

// Stop 停止 etcd 服务
func (es *EtcdService) Stop() {
	if es.client != nil {
		es.client.Close()
	}

	select {
	case es.stopChan <- true:
	default:
	}

	log.Println("EtcdService stopped")
}

// HealthCheck 检查 etcd 连接健康状况
func (es *EtcdService) HealthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := es.client.Status(ctx, es.config.Etcd.Endpoints[0])
	return err
}
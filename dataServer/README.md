# Mini-DFS DataServer

## 项目概述

Mini-DFS DataServer 是分布式文件系统中的数据存储节点，负责实际的数据块存储、副本管理和与元数据服务器的通信。

## 架构设计

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Client      │────│   MetaServer    │────│     etcd        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
    ┌────▼────┐              ┌───▼───┐               ┌───▼───┐
    │DataServer│◄────────────►│DataServer│◄─────────►│DataServer│
    └─────────┘              └───────┘               └───────┘
```

## 项目结构

```
dataServer/
├── cmd/dataServer/
│   └── main.go                 # 程序入口点
├── internal/
│   ├── handler/
│   │   └── grpc_handler.go     # gRPC服务实现
│   ├── service/
│   │   ├── storage_service.go      # 本地存储服务
│   │   ├── replication_service.go  # 数据复制服务
│   │   └── cluster_service.go      # 集群协调服务
│   └── model/
│       └── model.go             # 数据模型定义
├── pb/                          # protobuf相关文件
│   ├── types.go                # 消息类型定义
│   └── grpc.go                 # gRPC接口定义
├── config.yaml                 # 配置文件
└── go.mod                      # Go模块文件
```

## 核心功能

### 1. 数据存储 (StorageService)
- **块存储**: 使用哈希目录结构存储数据块，避免单目录文件过多
- **原子写入**: 通过临时文件确保写入的原子性
- **统计信息**: 提供磁盘使用情况、块数量等统计数据

### 2. 数据复制 (ReplicationService)  
- **前向复制**: 接收数据时同时转发给下一个副本节点
- **拉取复制**: 从其他节点拉取数据块进行复制
- **连接管理**: 维护到其他节点的gRPC连接池

### 3. 集群协调 (ClusterService)
- **服务注册**: 在etcd中注册服务信息
- **心跳机制**: 定期向MetaServer发送心跳
- **命令处理**: 执行MetaServer下发的删除、复制等命令

### 4. gRPC接口 (Handler)
- **WriteBlock**: 流式接收数据块并进行本地存储和转发复制
- **ReadBlock**: 流式发送数据块内容
- **DeleteBlock**: 删除指定的数据块
- **CopyBlock**: 从其他节点复制数据块

## 配置说明

```yaml
server:
  listen_address: "0.0.0.0:8001"    # 监听地址
  dataServer_id: "dataServer-01"    # 服务器唯一ID

storage:
  data_root_path: "./data"          # 数据存储根目录
  max_storage_size: 0               # 最大存储容量(0=无限制)

etcd:
  endpoints: ["localhost:2379"]     # etcd集群地址
  dial_timeout: 5                   # 连接超时(秒)
  lease_ttl: 30                     # 租约TTL(秒)

metaServer:
  address: "localhost:8000"         # MetaServer地址
  heartbeat_interval: 10            # 心跳间隔(秒)
  connection_timeout: 5             # 连接超时(秒)
```

## 数据流程

### 写入流程
1. 客户端向DataServer发送WriteBlock请求
2. 第一个消息包含元数据(blockID, 副本位置列表)
3. 后续消息包含数据块内容
4. DataServer并发执行:
   - 将数据写入本地存储
   - 转发数据到下一个副本节点
5. 返回写入结果

### 读取流程
1. 接收ReadBlock请求(包含blockID)
2. 从本地存储读取数据
3. 分块流式发送给客户端

### 复制流程
1. MetaServer通过心跳响应下发复制命令
2. DataServer从本地读取数据块
3. 通过ReplicationService转发到目标节点
4. 目标节点存储数据并返回结果

## 关键特性

### 高可靠性
- **原子操作**: 使用临时文件和重命名确保写入原子性
- **错误处理**: 完善的错误处理和日志记录
- **优雅关闭**: 支持信号处理和优雅关闭

### 高性能
- **并发处理**: 本地存储和复制转发并行执行
- **流式传输**: 使用gRPC流避免大文件内存占用
- **连接复用**: gRPC连接池减少连接开销

### 可扩展性
- **接口设计**: 清晰的服务接口便于扩展
- **配置驱动**: 通过配置文件控制行为
- **插件化**: 服务组件可独立替换

## 部署运行

### 1. 安装依赖
```bash
go mod download
```

### 2. 修改配置
编辑 `config.yaml` 文件，设置正确的地址和路径。

### 3. 启动服务
```bash
go run cmd/dataServer/main.go -config=config.yaml
```

### 4. 验证运行
检查日志输出，确认服务正常启动并注册到etcd。

## 监控和运维

### 日志信息
- 启动和关闭事件
- gRPC请求处理结果
- 存储操作统计
- 心跳和集群事件
- 错误和异常情况

### 运行状态
- 通过心跳机制向MetaServer报告状态
- 包括磁盘使用量、块数量、存储的块ID列表
- 支持优雅关闭，不丢失数据

## 故障处理

### 常见问题
1. **无法连接etcd**: 检查etcd服务状态和配置
2. **无法连接MetaServer**: 检查MetaServer地址和网络
3. **磁盘空间不足**: 监控磁盘使用情况
4. **副本同步失败**: 检查目标节点状态

### 恢复策略
- 服务重启后自动重新注册
- 心跳恢复后重新同步状态
- 支持手动触发数据块复制和修复
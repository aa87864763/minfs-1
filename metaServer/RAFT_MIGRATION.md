# Raft 架构迁移说明

## 概述

MetaServer 已从基于 **etcd Lease + 自定义 WAL** 的架构迁移到 **Hashicorp Raft** 共识算法。

---

## 架构对比

### 原架构（已废弃）
```
┌─────────────────────────────────────┐
│     etcd 集群（选举 + 服务发现）    │
│  - Leader Election (Campaign API)  │
│  - MetaServer 节点注册              │
│  - DataServer 服务发现              │
└──────────┬──────────────────────────┘
           │
    ┌──────┴──────┐
    │             │
┌───▼────┐   ┌───▼────┐
│ Meta-1 │   │ Meta-2 │
│(Leader)│   │(Follow)│
│        │   │        │
│ BadgerDB   │ BadgerDB│
│ + WAL  │──>│ + WAL  │  (手动 gRPC 同步)
└────────┘   └────────┘
```

**问题**：
- ❌ 需要手动实现 WAL 同步逻辑（SyncWAL RPC）
- ❌ 节点重连需要特殊处理（RequestWALSync）
- ❌ 一致性保证需要自己实现
- ❌ 复杂的故障恢复逻辑

---

### 新架构（Raft）
```
┌─────────────────────────────────────┐
│     etcd 集群（仅服务发现）         │
│  - MetaServer 节点注册 (新增)      │
│  - DataServer 服务发现 (保留)      │
└─────────────────────────────────────┘
           ▲
           │ 心跳注册
           │
┌──────────┴──────────────────────────┐
│       Raft 集群（共识 + 复制）      │
│                                     │
│  ┌────────┐  ┌────────┐  ┌────────┐│
│  │ Meta-1 │  │ Meta-2 │  │ Meta-3 ││
│  │(Leader)│◄─┤(Follow)│◄─┤(Follow)││
│  │        │  │        │  │        ││
│  │BadgerDB│  │BadgerDB│  │BadgerDB││
│  │+ Raft  │  │+ Raft  │  │+ Raft  ││
│  │(BoltDB)│  │(BoltDB)│  │(BoltDB)││
│  └────────┘  └────────┘  └────────┘│
│                                     │
│  Raft 自动处理日志复制和一致性      │
└─────────────────────────────────────┘
```

**优势**：
- ✅ **自动日志复制** - Raft 内部 AppendEntries RPC
- ✅ **强一致性保证** - Raft 共识算法
- ✅ **自动故障恢复** - Raft 自动选举新 Leader
- ✅ **简化代码** - 无需手动实现 WAL 同步
- ✅ **标准化** - 使用成熟的 Raft 库（hashicorp/raft）

---

## 已废弃的组件

### 1. 文件（已重命名为 .deprecated）
- ❌ `internal/service/wal_service.go` → 被 `RaftService` 替换
- ❌ `internal/service/leader_election.go` → 被 `RaftService` 替换

### 2. Proto RPC（已注释）
- ❌ `SyncWAL` - Raft 自动同步日志
- ❌ `RequestWALSync` - Raft 自动处理节点重连

### 3. Proto 消息（保留但标记废弃）
保留以下消息定义以兼容旧版 easyClient，但已不使用：
- `WALOperationType` 枚举
- `LogEntry` 消息
- `CreateNodeOperation` / `DeleteNodeOperation` 等操作消息
- `RequestWALSyncRequest`

---

## 新增组件

### 1. Raft 核心模块
```
internal/raft/
├── fsm.go           # Raft FSM 实现（应用日志到 BadgerDB）
├── log_entry.go     # Raft 日志条目定义
├── operations.go    # 操作数据结构
└── snapshot.go      # Raft 快照实现
```

### 2. RaftService
`internal/service/raft_service.go`
- 替代 `WALService` 和 `LeaderElection`
- 统一管理 Raft 共识、日志复制、Leader 选举

### 3. etcd 服务发现增强
`internal/service/etcd_service.go`
- 新增 `RegisterMetaServer()` - 所有节点注册到 etcd
- 新增 `UpdateMetaServerLeaderStatus()` - 更新 Leader 状态
- 新增 `UnregisterMetaServer()` - 节点下线注销

---

## 配置变更

### config.yaml 新增 Raft 配置
```yaml
raft:
  # 集群节点列表（用于 Bootstrap）
  peers:
    - id: "meta-9090"
      raft_addr: "localhost:10090"
      grpc_addr: "localhost:9090"
    - id: "meta-9091"
      raft_addr: "localhost:10091"
      grpc_addr: "localhost:9091"
    - id: "meta-9092"
      raft_addr: "localhost:10092"
      grpc_addr: "localhost:9092"
  
  # Raft 参数
  heartbeat_timeout: 1s
  election_timeout: 1s
  snapshot_interval: 120s
  snapshot_threshold: 8192
```

---

## 启动方式变更

### 原启动方式（已废弃）
```bash
# 启动单个节点
./metaServer --config config.yaml
```

### 新启动方式
```bash
# 启动第一个节点（Bootstrap）
./metaServer --config config.yaml --port 9090 --raft-port 10090 --bootstrap

# 启动其他节点（自动加入集群）
./metaServer --config config.yaml --port 9091 --raft-port 10091
./metaServer --config config.yaml --port 9092 --raft-port 10092
```

**注意**：
- `--bootstrap` 只在第一次启动集群时使用
- Bootstrap 节点会读取 `config.yaml` 中的所有 peers 并初始化集群
- 其他节点启动后自动加入已 bootstrap 的集群

---

## 数据存储变更

### 原数据存储
```
metadb/
└── badger/       # 元数据 + WAL
```

### 新数据存储
```
metadb/
├── badger/           # 元数据（通过 FSM 应用）
└── raft/
    ├── logs.db       # Raft WAL (BoltDB)
    ├── snapshots/    # Raft 快照
    └── stable.db     # Raft 稳定存储 (term, vote)
```

---

## API 兼容性

### 对 Client 的影响
✅ **完全兼容** - 所有 gRPC 接口保持不变：
- `CreateNode`
- `GetNodeInfo`
- `ListDirectory`
- `DeleteNode`
- `GetBlockLocations`
- `FinalizeWrite`
- `GetClusterInfo`

### 对 DataServer 的影响
✅ **完全兼容** - DataServer 接口保持不变：
- `Heartbeat`

### 废弃的接口
❌ 以下接口已废弃（调用会返回错误）：
- `SyncWAL` - 返回 "deprecated, Raft handles log replication automatically"
- `RequestWALSync` - 返回 "deprecated, Raft handles log replication automatically"

---

## 运维指南

### 检查集群状态
```bash
# 查看日志确认 Leader
tail -f workpublish/metaServer/logs/metaServer1.log | grep -i leader

# 预期输出
2025-11-30 14:42:56 ✓ This node is LEADER
2025-11-30 14:42:56 >>> This node became LEADER <<<
```

### 故障恢复
- **Leader 宕机**：Raft 自动选举新 Leader（~1-2 秒）
- **Follower 宕机**：重启后自动从 Leader 同步日志
- **网络分区**：少数派分区自动降级为 Follower

### etcd 服务发现
所有 MetaServer 节点注册到：
```
/minfs/metaServer/nodes/
├── meta-9090  # {"id":"meta-9090", "addr":"localhost:9090", "is_leader":true}
├── meta-9091  # {"id":"meta-9091", "addr":"localhost:9091", "is_leader":false}
└── meta-9092  # {"id":"meta-9092", "addr":"localhost:9092", "is_leader":false}
```

---

## 迁移检查清单

- [x] Raft 核心组件实现（fsm, log_entry, operations, snapshot）
- [x] RaftService 替换 WALService 和 LeaderElection
- [x] MetadataService 改用 RaftService.Apply()
- [x] ClusterService 适配 RaftService
- [x] gRPC Handler 废弃 WAL 方法
- [x] main.go 启动流程改造
- [x] config.yaml 添加 Raft 配置
- [x] build.sh 更新启动脚本
- [x] proto 文件标记废弃定义
- [x] 旧服务文件标记为 .deprecated
- [x] etcd 服务发现集成
- [ ] 集群测试（3 节点）
- [ ] 故障转移测试
- [ ] 性能测试

---

## 常见问题

### Q1: 为什么还保留 WAL 相关的 proto 定义？
A: 为了与旧版 easyClient 的兼容性。虽然服务端不再使用，但保留定义可以避免 proto 编译错误。

### Q2: etcd 还有什么作用？
A: 
- DataServer 服务发现（保留）
- MetaServer 节点注册和发现（新增）
- Raft 本身不依赖 etcd，有独立的共识机制

### Q3: 如何从旧架构迁移数据？
A: 暂不支持在线迁移。建议：
1. 备份旧 BadgerDB 数据
2. 使用新 Raft 架构启动集群
3. 通过 Client 重新导入数据

### Q4: 集群最少需要几个节点？
A: 建议 3 个节点（可容忍 1 个节点故障）。理论上 1 个节点也可运行，但失去高可用性。

---

## 参考资料

- [Hashicorp Raft 文档](https://github.com/hashicorp/raft)
- [Raft 论文](https://raft.github.io/raft.pdf)
- [etcd Raft 实现](https://github.com/etcd-io/raft)

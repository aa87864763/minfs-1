
# MetaServer 设计与实现文档

## 1. 目标

本文档旨在详细阐述 `MetaServer` 组件的设计思路与实现步骤。`MetaServer` 是分布式文件系统的核心元数据管理器，负责以下关键任务：

*   **命名空间管理**：维护文件系统的目录树结构。
*   **元数据存储**：持久化存储文件和目录的属性信息 (Inode)。
*   **数据块映射**：记录文件内容到具体数据块 (Block) 的映射关系。
*   **DataServer 调度**：管理所有 `DataServer` 节点的状态，并为新的写入操作智能地选择最佳的数据存储位置。
*   **系统健康维护**：通过心跳、FSCK 和垃圾回收机制，确保整个集群的健康和数据一致性。

本项目初期将实现一个单实例的 `MetaServer`，并为未来扩展到高可用集群（基于 Etcd）打下坚实基础。

## 2. 项目结构

为了保持与 `DataServer` 项目的一致性，我们将采用同样的分层结构：

```
/metaServer/
├── cmd/
│   └── metaServer/
│       └── main.go                // MetaServer 服务启动入口
├── internal/
│   ├── handler/
│   │   └── grpc_handler.go    // gRPC 请求的直接处理层
│   ├── service/
│   │   ├── metadata_service.go  // 封装 BadgerDB 操作，负责元数据 CRUD
│   │   ├── cluster_service.go   // 管理 DataServer 节点状态、心跳
│   │   └── scheduler_service.go // 负责块分配、FSCK、垃圾回收等调度策略
│   └── model/
│       └── model.go             // 定义核心的内存数据结构 (如 DataServer 状态)
├── pb/
│   ├── metaServer.pb.go
│   └── metaServer_grpc.pb.go
├── go.mod
├── go.sum
└── metaServer.md                  // 本文档
```

## 3. 核心设计

### 3.1. 元数据存储 (BadgerDB)

我们将使用 BadgerDB 作为元数据的 KV 存储引擎。其事务性保证了元数据操作的原子性。为了清晰地组织数据，我们设计以下 Key-Value 结构：

*   **Prefixes**:
    *   `i/` -> **Inodes**: 存储文件/目录的元数据。
    *   `p/` -> **Paths**: 存储路径到 Inode ID 的映射，用于快速查找。
    *   `d/` -> **Directory Entries**: 存储目录与其子项的父子关系。
    *   `b/` -> **Block Mappings**: 存储文件 Inode 到其数据块列表的映射。
    *   `gc/` -> **Garbage Collection**: 存储待回收的数据块 ID。

*   **Key-Value Schema**:
    *   **Inode**: `i/<inode_id>` -> `pb.NodeInfo` (序列化后的二进制数据)
    *   **Path Mapping**: `p/<full_path>` -> `<inode_id>` (64位整型)
    *   **Directory Entry**: `d/<parent_inode_id>/<child_name>` -> `<child_inode_id>` (64位整型)
    *   **Block Mapping**: `b/<file_inode_id>/<block_index>` -> `pb.BlockLocations` (序列化后的二进制数据)
    *   **GC Candidate**: `gc/<block_id>` -> `google.protobuf.Timestamp` (删除时间戳)

**原子事务**: 所有对元数据的修改（如 `CreateNode`）都必须在一个单独的 BadgerDB 事务 (`db.Update(...)`) 中完成。例如，创建一个新文件 `/a/b.txt` 需要原子地完成以下操作：
1.  生成新的 Inode ID。
2.  写入 `i/<new_inode_id>` -> `NodeInfo`。
3.  写入 `p//a/b.txt` -> `<new_inode_id>`。
4.  写入 `d/<parent_inode_id>/b.txt` -> `<new_inode_id>`。

### 3.2. DataServer 调度与负载均衡

这是 `MetaServer` 的大脑。当客户端请求写入文件时 (`GetBlockLocations`)，我们需要智能地为其分配数据块的存储位置。

**调度策略：带权重的轮询选择 (Weighted Round-Robin Selection)**

1.  **信息收集**: `MetaServer` 通过 `cluster_service` 维护一个实时的、健康的 `DataServer` 列表。每个 `DataServer` 的信息（来自心跳）包括：ID, 地址, 块数量, 剩余空间。
2.  **过滤**: 移除不健康的（长时间未心跳）或磁盘空间不足的 `DataServer`。
3.  **选择算法**:
    a. 从健康的 `DataServer` 列表中，选择出块数量 (`block_count`) 最少的 `N` 个节点（`N` 为副本数，例如 3）。这确保了数据被优先写入负载最低的节点。
    b. 为了避免每次都选择同样的几个节点，可以引入一个起始点指针，每次选择后都向前移动。
    c. **核心思想**：优先考虑负载（块数量），其次考虑轮询以保证公平性。这是一种简单而高效的负载均衡策略。
4.  **结果**: 算法返回一个 `pb.BlockLocations` 列表，其中每个 `BlockLocations` 都包含了为这个块选出的3个 `DataServer` 地址。

### 3.3. 系统健康维护 (FSCK & 垃圾回收)

#### FSCK (File System Check)

FSCK 是一个后台的、周期性运行的任务，用于检查和修复元数据与实际数据之间的不一致性。

*   **数据源**:
    1.  **元数据视图**: BadgerDB 中存储的所有 `b/` (Block Mappings)。这是“应该存在”的数据。
    2.  **物理数据视图**: 所有 `DataServer` 通过心跳上报的 `block_ids_report`。这是“实际存在”的数据。
*   **检查流程 (`scheduler_service`)**:
    1.  定期遍历所有元数据中的块，检查每个块的副本是否都能在对应的 `DataServer` 的块报告中找到。
    2.  **副本丢失 (Replica Loss)**: 如果元数据记录块 `B` 应该在 `DS1`, `DS2`, `DS3` 上，但 `DS2` 的心跳报告中没有块 `B`，则判定 `B` 在 `DS2` 上丢失了一个副本。
    3.  **修复**: `MetaServer` 会向 `DS2` 发送一个 `COPY_BLOCK` 指令（通过心跳响应），让它从 `DS1` 或 `DS3` 拉取数据，从而恢复副本数。
    4.  **孤儿块 (Orphan Block)**: 如果 `DS4` 的报告中有一个块 `O`，但在元数据中找不到任何文件拥有这个块，则 `O` 是一个孤儿块。
    5.  **处理**: `MetaServer` 会向 `DS4` 发送 `DELETE_BLOCK` 指令，回收这些无效数据。

#### 异步垃圾回收 (Garbage Collection)

当一个文件被删除时，它所占用的数据块不会被立即删除。

*   **流程**:
    1.  `DeleteNode` 请求被调用时，`MetaServer` 在一个原子事务中删除文件的元数据，并将其所有数据块的 ID 写入到 `gc/` 前缀下。
    2.  `scheduler_service` 中有一个后台 goroutine，定期扫描 `gc/` 中的待删块。
    3.  对于每个待删块，`MetaServer` 会找到拥有该块的 `DataServer`，并通过心跳响应向它们发送 `DELETE_BLOCK` 指令。
    4.  当 `DataServer` 在下次心跳中不再报告这个块 ID 后，`MetaServer` 从 `gc/` 中移除该条目。
*   **优点**: 这种异步机制将文件删除的元数据操作与耗时的数据块物理删除操作解耦，使得 `DeleteNode` 接口可以快速响应。

## 4. 接口实现思路

*   **`Heartbeat`**: 这是 `MetaServer` 与 `DataServer` 交互的核心。`cluster_service` 接收心跳，更新 `DataServer` 的状态（活跃时间、负载信息、块列表），并从 `scheduler_service` 获取待下发的指令（如 `COPY_BLOCK`, `DELETE_BLOCK`）并返回。
*   **`CreateNode`**: 由 `metadata_service` 处理，在 BadgerDB 事务中创建 Inode 和路径映射。
*   **`GetBlockLocations`**: `handler` 调用 `scheduler_service` 的负载均衡算法来获取块的位置，然后调用 `metadata_service` 在 BadgerDB 中预创建（或更新）文件的块映射信息。
*   **`FinalizeWrite`**: 客户端完成数据写入后调用。`metadata_service` 会更新对应 Inode 的最终文件大小和修改时间。
*   **`DeleteNode`**: `metadata_service` 在事务中删除元数据，并将待删除的块 ID 交给 `scheduler_service` 的垃圾回收模块处理。
*   **`ListDirectory`**: `metadata_service` 根据 `d/` 前缀查询指定目录下的所有子节点，并聚合它们的 `NodeInfo` 返回。

## 5. 实现步骤 (Roadmap)

1.  **环境搭建**:
    *   在 `go.mod` 中添加 `github.com/dgraph-io/badger/v3` 依赖。
    *   使用 `protoc` 基于 `metaServer.proto` 生成 Go gRPC 代码到 `pb/` 目录。

2.  **基础服务层开发 (`internal/metaServer/service`)**:
    *   **`metadata_service.go`**: 实现对 BadgerDB 的所有操作的封装，提供如 `CreateNode`, `GetNode`, `DeleteNodeInTx` 等方法。
    *   **`cluster_service.go`**: 实现 `DataServer` 注册、心跳处理和状态管理逻辑。定义 `DataServerInfo` 结构体来存储内存中的状态。

3.  **调度逻辑开发 (`internal/metaServer/service`)**:
    *   **`scheduler_service.go`**: 实现核心的负载均衡算法。初期可以先实现 FSCK 和 GC 的基本框架。

4.  **Handler 和 Main 函数开发**:
    *   **`grpc_handler.go`**: 实现 `MetaServerService` 接口，将 gRPC 请求路由到下面各个 `service` 的具体方法。
    *   **`main.go`**: 编写启动逻辑，包括：
        *   初始化 BadgerDB。
        *   实例化所有 `service`。
        *   启动 gRPC 服务器。
        *   启动 `scheduler_service` 中的后台任务（FSCK, GC）。

5.  **测试**:
    *   编写单元测试，特别是针对 `metadata_service` 和 `scheduler_service` 的调度算法。
    *   修改 `three_replica.go` 或创建一个新的测试客户端，使其能够与 `MetaServer` 交互来完成端到端的写入、读取和删除测试。

## 6. 未来展望

当单实例功能完善后，可以引入 `etcd` 来实现 `MetaServer` 的高可用：
*   **主从选举**: 所有 `MetaServer` 实例通过 `etcd` 的租约（Lease）和选举机制选举出一个 Leader。
*   **状态同步**: 只有 Leader 对外提供服务。所有的写操作都需要先写入一个基于 `etcd` 的 WAL (Write-Ahead Log)，然后才应用到 BadgerDB。Follower 节点监听 WAL 并同步更新自己的状态。
*   **故障切换**: 当 Leader 宕机，其他节点会通过 `etcd` 感知到，并自动选举出新的 Leader，实现无缝切换。

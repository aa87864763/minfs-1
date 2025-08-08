# MiniFS 分布式文件系统

MiniFS 是一个简单的分布式文件系统，采用 MetaServer + DataServer 架构，支持文件的分布式存储、三副本机制、数据完整性验证和集群管理。

## 系统架构

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client    │    │ MetaServer  │    │   etcd      │
│             │◄──►│             │◄──►│             │
└─────────────┘    └─────────────┘    └─────────────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
    ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
    │ DataServer1 │   │ DataServer2 │   │ DataServer3 │
    └─────────────┘   └─────────────┘   └─────────────┘
```

## 核心功能

### 基础功能
- ✅ 文件创建 (create)
- ✅ 目录创建 (mkdir)
- ✅ 文件/目录查看属性 (stat/ls)
- ✅ 文件/目录删除 (rm)
- ✅ 文件读写 (read/write)

### 高级功能
- ✅ 三副本存储机制
- ✅ MD5 数据完整性验证
- ✅ FSCK 文件系统检查和修复
- ✅ 集群状态监控
- ✅ 心跳机制 (10s间隔, 20s超时)
- ✅ 垃圾回收机制

## 客户端命令参考

### 连接服务器
```bash
./client <metaserver_address>
```

### 基本命令

#### 帮助命令
```bash
minifs> help                    # 显示所有可用命令
minifs> h                       # 同上
```

#### 退出命令
```bash
minifs> exit                    # 退出客户端
minifs> quit                    # 同上
minifs> q                       # 同上
```

### 文件系统操作

#### 文件操作
```bash
# 创建文件
minifs> create /test/hello.txt

# 写入文件内容
minifs> write /test/hello.txt "Hello World!"
minifs> write /test/hello.txt Hello World Without Quotes

# 读取文件内容  
minifs> read /test/hello.txt

# 查看文件状态（包含 MD5 校验值）
minifs> stat /test/hello.txt

# 删除文件
minifs> rm /test/hello.txt
```

#### 目录操作
```bash
# 创建目录
minifs> mkdir /test
minifs> mkdir /test/subdir

# 列出目录内容
minifs> ls /
minifs> ls /test

# 删除空目录
minifs> rm /test/subdir

# 递归删除目录及其内容
minifs> rm /test recursive
```

#### 文件传输
```bash
# 上传本地文件到分布式文件系统
minifs> put /path/to/local/file.txt /remote/path/file.txt

# 下载文件到本地
minifs> get /remote/path/file.txt /path/to/local/downloaded.txt
```

### 集群管理

#### 集群状态
```bash
# 查看集群整体状态
minifs> cluster
```

#### 副本状态
```bash
# 查看所有文件的副本状态
minifs> replicas

# 查看特定文件的副本状态  
minifs> replicas /test/hello.txt
minifs> repl /test/hello.txt        # 简写形式
```

## 系统特性

### 数据完整性
- **MD5 校验**: 每个文件存储时自动计算 MD5 哈希值
- **读取验证**: 读取文件时重新计算 MD5 并与存储值比较
- **完整性报告**: `stat` 命令显示文件的 MD5 校验值

### 高可用性
- **三副本机制**: 每个数据块默认存储 3 个副本
- **FSCK 检查**: 定期检查副本完整性，自动修复丢失副本
- **心跳监控**: DataServer 每 10 秒向 MetaServer 发送心跳
- **故障检测**: 20 秒心跳超时后标记节点为不可用

### 存储优化
- **块存储**: 文件按块分割存储，支持大文件
- **简化命名**: 数据块文件名直接使用毫秒时间戳 (如: `1754633869123.dat`)
- **单级目录**: DataServer 采用扁平存储结构，便于管理

## 配置要求

### 系统配置
- **心跳间隔**: 10 秒
- **心跳超时**: 20 秒  
- **FSCK 周期**: 100 秒
- **默认副本数**: 3
- **块大小**: 可配置

### 端口分配
- **MetaServer**: 9090 (默认)
- **DataServer**: 8001, 8002, 8003, 8004 (示例)
- **etcd**: 2379 (客户端), 2380 (集群通信)

## 使用示例

### 完整操作流程
```bash
# 1. 连接到 MetaServer
./client localhost:9090

# 2. 创建目录结构
minifs> mkdir /test
minifs> mkdir /test/documents

# 3. 创建和写入文件
minifs> create /test/hello.txt
minifs> write /test/hello.txt "Hello MiniFS! This is a test file."

# 4. 查看文件信息
minifs> stat /test/hello.txt
# 输出包含: 文件大小、修改时间、MD5 校验值

# 5. 读取文件内容
minifs> read /test/hello.txt

# 6. 检查副本状态
minifs> replicas /test/hello.txt
# 显示文件在各 DataServer 上的分布情况

# 7. 查看集群状态  
minifs> cluster

# 8. 列出目录内容
minifs> ls /test

# 9. 上传本地文件
minifs> put ./README.md /test/readme.md

# 10. 下载文件
minifs> get /test/readme.md ./downloaded_readme.md

# 11. 清理
minifs> rm /test recursive
```

### 故障恢复测试
```bash
# 查看文件副本分布
minifs> replicas /test/important.txt

# 模拟 DataServer 故障后，FSCK 会自动检测并修复
# 等待 100 秒后再次检查副本状态
minifs> replicas /test/important.txt
```

## 注意事项

1. **路径格式**: 所有路径必须以 `/` 开头 (绝对路径)
2. **文件创建**: 必须先使用 `create` 创建文件，再使用 `write` 写入内容
3. **递归删除**: 删除非空目录时必须添加 `recursive` 参数
4. **数据完整性**: 系统自动进行 MD5 校验，无需手动干预
5. **集群监控**: 使用 `cluster` 和 `replicas` 命令定期检查系统状态

## 错误处理

常见错误及解决方法：
- **文件不存在**: 确认文件路径正确，使用 `ls` 检查目录内容
- **目录非空**: 删除目录时使用 `recursive` 参数
- **权限错误**: 检查文件系统权限和 DataServer 状态
- **网络错误**: 确认 MetaServer 和 DataServer 服务正常运行
- **副本不足**: 等待 FSCK 自动修复，或检查 DataServer 健康状态
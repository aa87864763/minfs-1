# MinFS Java客户端

这是MinFS分布式文件系统的Java客户端，提供了与Go客户端相同的命令行接口。

## 特性

- ✅ **自动服务发现**: 通过etcd自动发现MinFS集群
- ✅ **Leader故障转移**: 自动检测Leader节点并支持故障转移
- ✅ **完整的文件系统操作**: 支持文件/目录的创建、删除、读写等操作
- ✅ **集群监控**: 查看集群状态和文件副本分布
- ✅ **兼容Go客户端**: 命令语法与Go客户端完全一致

## 构建

### 前提条件

- Java 17 或更高版本
- Maven 3.6+
- 运行中的MinFS集群（MetaServer + DataServer）
- etcd服务

### 构建JAR包

```bash
# 进入客户端目录
cd /home/wyz/project/golang/minfs-1/client

# 清理并构建
mvn clean package -DskipTests

# 生成的JAR文件
# - target/easyClient-1.0.jar (普通JAR)
# - target/minfs-client-1.0.jar (包含所有依赖的可执行JAR)
```

## 使用方式

### 1. 启动脚本

#### Linux/Mac
```bash
# 使用启动脚本
./minfs-client.sh

# 或直接运行JAR
java -jar target/minfs-client-1.0.jar
```

#### Windows
```cmd
REM 使用启动脚本
minfs-client.bat

REM 或直接运行JAR
java -jar target/minfs-client-1.0.jar
```

### 2. 客户端配置

客户端默认使用以下配置连接MinFS集群：
- **etcd地址**: `http://localhost:2379`
- **服务路径**: `/minfs`
- **自动Leader发现**: 启用

如需修改配置，请编辑 `EFileSystem` 构造函数中的参数：
```java
// 在 MinFSCLI.java 中
this.fileSystem = new EFileSystem("minfs");
```

## 命令参考

启动客户端后，您将看到 `minfs>` 提示符。以下是可用的命令：

### 基础命令

```bash
help                      # 显示帮助信息
exit/quit/q              # 退出客户端
```

### 文件操作

```bash
# 创建文件和目录
create <path>            # 创建文件
mkdir <path>             # 创建目录

# 查看和浏览
ls <path>                # 列出目录内容
stat <path>              # 显示文件/目录状态

# 删除操作
rm <path>                # 删除文件/目录 (递归)
```

### 数据操作

```bash
# 文件读写
write <path> <text>      # 向文件写入文本
read <path>              # 读取文件内容

# 文件上传下载
put <local_file> <remote_path>  # 上传本地文件
get <remote_path> <local_file>  # 下载文件
```

### 集群监控

```bash
# 集群信息
cluster                  # 显示集群信息

# 副本监控
replicas [path]          # 显示文件副本状态
                        # 不指定路径则显示所有文件
```

## 使用示例

### 基本文件操作

```bash
minfs> mkdir /test
✅ 目录创建成功: /test

minfs> create /test/hello.txt
✅ 文件创建成功: /test/hello.txt

minfs> write /test/hello.txt "Hello MinFS!"
✅ 写入成功: /test/hello.txt (12 字节)

minfs> read /test/hello.txt
文件内容 /test/hello.txt:
--- 开始 ---
Hello MinFS!
--- 结束 ---
✅ 读取成功 (12 字节)

minfs> stat /test/hello.txt
=== 文件状态信息 ===
路径: /test/hello.txt
类型: File
大小: 12 字节
修改时间: 1692087524000
MD5: 5d41402abc4b2a76b9719d911017c592

minfs> ls /test
目录 /test 的内容:
  FILE /test/hello.txt (大小: 12, 修改时间: 1692087524000)
✅ 共 1 个项目
```

### 文件上传下载

```bash
minfs> put /local/path/document.pdf /remote/documents/document.pdf
✅ 上传成功: /local/path/document.pdf -> /remote/documents/document.pdf (102400 字节)

minfs> get /remote/documents/document.pdf /local/backup/document.pdf
✅ 下载成功: /remote/documents/document.pdf -> /local/backup/document.pdf (102400 字节)
```

### 集群监控

```bash
minfs> cluster
=== 集群信息 ===
主MetaServer: localhost:9090
从MetaServer数量: 2
DataServer数量: 3
DataServer详情:
  1. localhost:8080 (文件: 25, 容量: 512/1024MB)
  2. localhost:8081 (文件: 30, 容量: 768/1024MB)
  3. localhost:8082 (文件: 20, 容量: 256/1024MB)
✅ 集群信息获取成功

minfs> replicas /test/hello.txt
=== 副本分布信息 ===
总文件数: 1
健康文件数: 1
副本不足文件数: 0
副本过多文件数: 0

文件副本详情:
  文件: /test/hello.txt
    期望副本数: 3
    实际副本数: 3
    健康状态: HEALTHY
    数据块分布:
      块ID 12345 -> [localhost:8080, localhost:8081, localhost:8082] (副本数: 3)
✅ 副本信息获取成功
```

## 故障转移特性

客户端具备自动故障转移能力：

1. **自动Leader发现**: 客户端启动时会自动发现当前的Leader MetaServer
2. **故障检测**: 当与Leader的连接失败时，自动重新发现新的Leader
3. **操作重试**: 支持对失败的操作进行自动重试
4. **透明切换**: 故障转移过程对用户透明

## 日志输出

客户端会输出详细的操作日志，包括：
- 连接状态
- 操作结果
- 错误信息
- 故障转移过程

## 故障排除

### 常见问题

1. **连接失败**
   ```
   ❌ 客户端初始化失败
   ```
   - 检查etcd服务是否运行
   - 确认MinFS集群是否启动
   - 验证网络连接

2. **操作超时**
   ```
   ❌ 操作失败: DEADLINE_EXCEEDED
   ```
   - 检查集群负载
   - 确认DataServer状态
   - 增加操作超时时间

3. **文件不存在**
   ```
   ❌ 文件不存在: /path/to/file
   ```
   - 使用 `ls` 命令检查路径
   - 确认文件路径正确

### 调试信息

启用调试模式：
```bash
java -Djava.util.logging.level=INFO -jar target/minfs-client-1.0.jar
```

## 性能建议

1. **大文件传输**: 建议使用 `put` 和 `get` 命令而不是 `write` 和 `read`
2. **批量操作**: 将多个小文件操作合并为单次传输
3. **网络环境**: 确保客户端与集群之间的网络延迟较低

## 与Go客户端的对比

| 特性 | Java客户端 | Go客户端 | 兼容性 |
|------|------------|----------|--------|
| 命令语法 | ✅ 完全相同 | ✅ | ✅ |
| 服务发现 | ✅ etcd | ✅ etcd | ✅ |
| Leader故障转移 | ✅ 自动 | ✅ 自动 | ✅ |
| 文件操作 | ✅ 完整支持 | ✅ 完整支持 | ✅ |
| 集群监控 | ✅ 支持 | ✅ 支持 | ✅ |
| 启动速度 | 较慢 (JVM) | 快 | - |
| 内存占用 | 较高 | 较低 | - |

## 开发信息

- **开发语言**: Java 17
- **构建工具**: Maven 3.6+
- **主要依赖**: 
  - gRPC Java 1.57.2
  - etcd-jetcd 0.7.7
  - Protocol Buffers 3.24.4

## 许可证

此项目与MinFS主项目使用相同的许可证。
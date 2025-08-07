# MiniFS Client

MiniFS分布式文件系统的客户端实现，提供完整的文件系统操作接口和命令行交互界面。

## 功能特性

### A1: 文件和目录创建
- `create <path>` - 创建文件
- `mkdir <path>` - 创建目录

### A2: 查看属性信息  
- `stat <path>` - 获取文件/目录的详细信息
- `ls <path>` - 列出目录下的所有内容

### A3: 删除操作
- `rm <path> [recursive]` - 删除文件或目录，支持递归删除

### A4: 文件操作
- `write <path> <text>` - 写入文本到文件
- `read <path>` - 读取文件内容
- `put <local_file> <remote_path>` - 上传本地文件
- `get <remote_path> <local_file>` - 下载文件
- 支持MD5校验确保数据完整性

### A5: 集群信息
- `cluster` - 获取集群状态信息

## 快速开始

### 1. 启动客户端

```bash
cd client
./start_client.sh localhost:8080
```

这将自动构建并启动交互式命令行客户端。

### 2. 基本操作示例

```
minifs> help                           # 查看帮助
minifs> cluster                        # 查看集群信息
minifs> mkdir /test                    # 创建目录
minifs> create /test/hello.txt         # 创建文件
minifs> write /test/hello.txt "Hello MiniFS!"  # 写入内容
minifs> read /test/hello.txt           # 读取内容
minifs> stat /test/hello.txt           # 查看文件状态
minifs> ls /test                       # 列出目录内容
minifs> rm /test/hello.txt             # 删除文件
minifs> rm /test recursive             # 递归删除目录
minifs> exit                           # 退出客户端
minifs> put local_file.txt /test/hello.txt
minifs> get /test/hello.txt local_file.txt
```

### 3. 大文件测试

使用文件生成工具创建任意大小的测试文件：

```bash
# 生成不同大小的测试文件
./bin/filegen test_1MB.dat 1MB random     # 1MB随机数据
./bin/filegen test_10MB.txt 10MB text     # 10MB文本数据
./bin/filegen test_100MB.bin 100MB zeros  # 100MB零数据
./bin/filegen test_1GB.dat 1GB pattern    # 1GB模式数据
```

然后在客户端中上传测试：

```
minifs> put test_1MB.dat /test/1mb.dat
minifs> put test_10MB.txt /test/10mb.txt
minifs> stat /test/1mb.dat
minifs> get /test/1mb.dat downloaded_1mb.dat
```

## 工具说明

### 客户端命令

- `help` - 显示所有可用命令
- `cluster` - 获取集群信息
- `mkdir <path>` - 创建目录
- `create <path>` - 创建文件
- `ls <path>` - 列出目录内容
- `stat <path>` - 显示文件/目录状态
- `write <path> <text>` - 写入文本
- `read <path>` - 读取文件
- `put <local> <remote>` - 上传文件
- `get <remote> <local>` - 下载文件
- `rm <path> [recursive]` - 删除文件/目录
- `exit/quit/q` - 退出客户端

### 文件生成工具

语法：`./bin/filegen <filename> <size> [mode]`

**大小格式：**
- `123` 或 `123B` - 字节
- `123K` 或 `123KB` - 千字节
- `123M` 或 `123MB` - 兆字节  
- `123G` 或 `123GB` - 千兆字节

**生成模式：**
- `random` - 随机二进制数据（默认）
- `text` - 可读文本内容
- `zeros` - 全零数据
- `pattern` - 重复模式数据

**示例：**
```bash
./bin/filegen small.txt 1KB text      # 1KB文本文件
./bin/filegen medium.bin 1MB random   # 1MB随机文件
./bin/filegen large.dat 100MB zeros   # 100MB零文件
./bin/filegen huge.bin 1GB pattern    # 1GB模式文件
```

## 项目结构

```
client/
├── cmd/
│   ├── client/          # 交互式客户端
│   │   └── main.go
│   └── filegen/         # 文件生成工具
│       └── main.go
├── pkg/client/          # 客户端核心实现
│   └── client.go
├── pb/                  # Protocol Buffer生成的文件
├── bin/                 # 编译输出目录
├── go.mod              # Go模块定义
├── start_client.sh     # 客户端启动脚本
├── test_client.sh      # 自动化测试脚本
└── README.md           # 本文档
```

## 测试验证

### 自动化测试
```bash
./test_client.sh localhost:8080
```

### 手动测试各项功能

1. **A5 - 集群信息**：`cluster`
2. **A1 - 创建操作**：`mkdir /test`, `create /test/file.txt`
3. **A2 - 状态查询**：`stat /test`, `ls /test`
4. **A4 - 文件操作**：`write`, `read`, `put`, `get`，自动MD5校验
5. **A3 - 删除操作**：`rm /test/file.txt`, `rm /test recursive`

### 大文件测试流程

1. 生成测试文件：`./bin/filegen large.dat 50MB random`
2. 启动客户端：`./start_client.sh localhost:8080`
3. 上传文件：`put large.dat /large_test.dat`
4. 检查状态：`stat /large_test.dat`
5. 下载文件：`get /large_test.dat downloaded.dat`
6. 对比MD5验证数据完整性

## 注意事项

1. 运行前确保MetaServer和DataServer服务已启动
2. 默认连接地址为`localhost:8080`，可通过参数指定其他地址
3. 所有文件操作都会显示MD5校验结果
4. 支持任意大小文件的读写测试
5. 使用`Ctrl+C`或`exit`命令退出客户端
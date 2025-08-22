#!/bin/bash

# MinFS 项目构建脚本
# 生成符合规范的 workpublish 目录结构

set -e

echo "=== MinFS 项目构建开始 ==="

# 定义变量
PROJECT_ROOT=$(pwd)
WORKPUBLISH_DIR="$PROJECT_ROOT/workpublish"

# 清理并创建 workpublish 目录结构
echo "创建 workpublish 目录结构..."
rm -rf "$WORKPUBLISH_DIR"
mkdir -p "$WORKPUBLISH_DIR"/{bin,metaServer,dataServer,easyClient}
mkdir -p "$WORKPUBLISH_DIR/metaServer"/{lib,logs,pid,etcd-data}
mkdir -p "$WORKPUBLISH_DIR/dataServer"/{lib,logs,pid}

echo "=== 1. 编译 MetaServer ==="
cd "$PROJECT_ROOT/metaServer"
if [ ! -f "go.mod" ]; then
    echo "错误: metaServer/go.mod 不存在"
    exit 1
fi

# 编译 metaServer
go build -o "$WORKPUBLISH_DIR/metaServer/metaServer" ./cmd/metaServer
echo "MetaServer 编译完成"

# 复制 metaServer 配置文件
if [ -f "config.yaml" ]; then
    cp config.yaml "$WORKPUBLISH_DIR/metaServer/"
fi

echo "=== 2. 编译 DataServer ==="
cd "$PROJECT_ROOT/dataServer"
if [ ! -f "go.mod" ]; then
    echo "错误: dataServer/go.mod 不存在"
    exit 1
fi

# 编译 dataServer
go build -o "$WORKPUBLISH_DIR/dataServer/dataServer" ./cmd/dataServer
echo "DataServer 编译完成"

# 复制 dataServer 配置文件
if [ -f "config.yaml" ]; then
    cp config.yaml "$WORKPUBLISH_DIR/dataServer/"
fi

echo "=== 3. 编译 EasyClient ==="
cd "$PROJECT_ROOT/easyClient"
if [ ! -f "pom.xml" ]; then
    echo "错误: easyClient/pom.xml 不存在"
    exit 1
fi

# 编译 client jar
mvn clean package -DskipTests
if [ -f "target/easyClient-1.0.jar" ]; then
    cp target/easyClient-1.0.jar "$WORKPUBLISH_DIR/easyClient/easyClient-1.0.jar"
    echo "EasyClient-1.0.jar (包含所有依赖) 生成完成"
else
    echo "错误: easyClient-1.0.jar 生成失败"
    exit 1
fi

echo "=== 4. 创建启动脚本 ==="

# SDK JAR包已复制到 workpublish/easyClient/ 目录

# 创建集群启动脚本  
cat > "$WORKPUBLISH_DIR/bin/start.sh" << 'EOF'
#!/bin/bash

# MinFS 集群一键启动脚本
# 调用 workpublish 中的二进制文件启动多实例集群

set -e

# 颜色输出定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 路径定义
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKPUBLISH_DIR="$(dirname "$SCRIPT_DIR")"
METASERVER_DIR="$WORKPUBLISH_DIR/metaServer"
DATASERVER_DIR="$WORKPUBLISH_DIR/dataServer"

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# 检查进程是否运行
check_process() {
    local pid_file=$1
    local service_name=$2
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "$service_name is already running (PID: $pid)"
            return 0
        else
            log_warn "Stale PID file found for $service_name, removing..."
            rm -f "$pid_file"
        fi
    fi
    return 1
}

# 等待服务启动
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=30
    local attempt=1
    
    log_step "Waiting for $service_name to start on $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z "$host" "$port" 2>/dev/null; then
            log_info "$service_name is ready!"
            return 0
        fi
        
        printf "."
        sleep 1
        attempt=$((attempt + 1))
    done
    
    echo
    log_error "$service_name failed to start within $max_attempts seconds"
    return 1
}

# 检查二进制文件
check_binaries() {
    log_step "Checking binary files..."
    
    if [ ! -f "$METASERVER_DIR/metaServer" ]; then
        log_error "MetaServer binary not found at $METASERVER_DIR/metaServer"
        log_error "Please run build.sh first"
        exit 1
    fi
    
    if [ ! -f "$DATASERVER_DIR/dataServer" ]; then
        log_error "DataServer binary not found at $DATASERVER_DIR/dataServer"
        log_error "Please run build.sh first"
        exit 1
    fi
    
    log_info "All binary files found"
}

# 启动 etcd
start_etcd() {
    log_step "Starting etcd..."
    
    if check_process "$METASERVER_DIR/pid/etcd.pid" "etcd"; then
        return 0
    fi
    
    # 检查 etcd 是否已安装
    if ! command -v etcd &> /dev/null; then
        log_error "etcd is not installed. Please install etcd first."
        exit 1
    fi
    
    # 获取本机IP地址，优先使用127.0.0.1
    local_ip="127.0.0.1"
    
    # 启动 etcd
    nohup etcd \
        --name=etcd-node \
        --data-dir="$METASERVER_DIR/etcd-data" \
        --listen-client-urls=http://0.0.0.0:2379 \
        --advertise-client-urls=http://$local_ip:2379 \
        --listen-peer-urls=http://0.0.0.0:2380 \
        --initial-advertise-peer-urls=http://$local_ip:2380 \
        --initial-cluster=etcd-node=http://$local_ip:2380 \
        --initial-cluster-state=new \
        --initial-cluster-token=minfs-cluster \
        > "$METASERVER_DIR/logs/etcd.log" 2>&1 &
    
    echo $! > "$METASERVER_DIR/pid/etcd.pid"
    
    # 等待 etcd 启动
    wait_for_service "localhost" "2379" "etcd"
}

# 启动 MetaServers (3个实例，1主2从)
start_metaServers() {
    log_step "Starting MetaServers (3 instances for HA)..."
    
    cd "$METASERVER_DIR"
    
    for i in {1..3}; do
        local port=$((9089 + i))
        local instance="metaServer${i}"
        local node_id="metaServer-${port}"
        local data_dir="./metadb${i}"
        
        if check_process "$METASERVER_DIR/pid/${instance}.pid" "MetaServer-${i}"; then
            continue
        fi
        
        log_info "Starting MetaServer-${i} on port ${port}..."
        
        # 确保数据目录存在
        mkdir -p "${data_dir}"
        
        nohup ./metaServer \
            -config=config.yaml \
            -port=${port} \
            -node-id="${node_id}" \
            -data-dir="${data_dir}" \
            > "logs/${instance}.log" 2>&1 &
        
        echo $! > "pid/${instance}.pid"
        
        # 等待 MetaServer 启动
        wait_for_service "localhost" "${port}" "MetaServer-${i}"
        
        sleep 1  # 稍微延迟，避免同时启动造成资源竞争
    done
    
    log_info "All MetaServers started successfully!"
}

# 启动 DataServers (4个实例)
start_dataServers() {
    log_step "Starting DataServers (4 instances)..."
    
    cd "$DATASERVER_DIR"
    
    for i in {1..4}; do
        local port=$((8000 + i))
        local instance="dataServer${i}"
        
        if check_process "$DATASERVER_DIR/pid/${instance}.pid" "DataServer-${i}"; then
            continue
        fi
        
        log_info "Starting DataServer-${i} on port ${port}..."
        
        # 确保数据目录存在
        mkdir -p "data${i}"
        
        nohup ./dataServer \
            -config=config.yaml \
            -port=${port} \
            -id=${i} \
            > "logs/${instance}.log" 2>&1 &
        
        echo $! > "pid/${instance}.pid"
        
        # 等待 DataServer 启动
        wait_for_service "localhost" "$port" "DataServer-${i}"
        
        sleep 1  # 稍微延迟，避免同时启动造成资源竞争
    done
    
    log_info "All DataServers started successfully!"
}

# 显示集群状态
show_cluster_status() {
    echo
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}      MinFS Cluster Status${NC}"
    echo -e "${CYAN}========================================${NC}"
    
    # 检查各个服务状态
    services=(
        "etcd:$METASERVER_DIR/pid/etcd.pid:2379"
        "MetaServer-1:$METASERVER_DIR/pid/metaServer1.pid:9090"
        "MetaServer-2:$METASERVER_DIR/pid/metaServer2.pid:9091"
        "MetaServer-3:$METASERVER_DIR/pid/metaServer3.pid:9092"
        "DataServer-1:$DATASERVER_DIR/pid/dataServer1.pid:8001"
        "DataServer-2:$DATASERVER_DIR/pid/dataServer2.pid:8002"
        "DataServer-3:$DATASERVER_DIR/pid/dataServer3.pid:8003"
        "DataServer-4:$DATASERVER_DIR/pid/dataServer4.pid:8004"
    )
    
    for service in "${services[@]}"; do
        IFS=':' read -r name pid_file port <<< "$service"
        
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if ps -p "$pid" > /dev/null 2>&1; then
                if nc -z localhost "$port" 2>/dev/null; then
                    echo -e "${GREEN}✓${NC} $name (PID: $pid, Port: $port) - ${GREEN}Running${NC}"
                else
                    echo -e "${YELLOW}⚠${NC} $name (PID: $pid, Port: $port) - ${YELLOW}Process exists but port not accessible${NC}"
                fi
            else
                echo -e "${RED}✗${NC} $name - ${RED}Not running${NC}"
            fi
        else
            echo -e "${RED}✗${NC} $name - ${RED}Not started${NC}"
        fi
    done
    
    echo
    echo -e "${CYAN}MetaServer logs: $METASERVER_DIR/logs/${NC}"
    echo -e "${CYAN}DataServer logs: $DATASERVER_DIR/logs/${NC}"
    echo -e "${CYAN}Client JAR: $WORKPUBLISH_DIR/easyClient/easyClient-1.0.jar${NC}"
    echo
}

# 主函数
main() {
    echo -e "${PURPLE}"
    echo "╔═══════════════════════════════════════════════════╗"
    echo "║               MinFS Cluster Startup              ║"
    echo "║                                                   ║"
    echo "║    Starting from workpublish binary files        ║"
    echo "╚═══════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    log_info "Starting MinFS cluster from workpublish..."
    log_info "Workpublish directory: $WORKPUBLISH_DIR"
    
    # 检查二进制文件
    check_binaries
    
    # 按顺序启动服务
    start_etcd
    sleep 2
    
    start_metaServers
    sleep 5  # 给metaServer选举更多时间
    
    start_dataServers
    sleep 2
    
    # 显示集群状态
    show_cluster_status
    
    echo -e "${GREEN}✅ MinFS cluster started successfully!${NC}"
    echo
    echo -e "${YELLOW}To use SDK: java -cp $WORKPUBLISH_DIR/easyClient/easyClient-1.0.jar YourMainClass${NC}"
    echo -e "${YELLOW}To stop cluster: $SCRIPT_DIR/stop.sh${NC}"
    echo
}

# 处理中断信号
cleanup() {
    log_warn "Received interrupt signal, stopping cluster..."
    "$SCRIPT_DIR/stop.sh"
    exit 0
}

trap cleanup INT TERM

# 运行主函数
main "$@"
EOF

# 创建集群停止脚本
cat > "$WORKPUBLISH_DIR/bin/stop.sh" << 'EOF'
#!/bin/bash

# MinFS 集群停止脚本

set -e

# 颜色输出定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 路径定义
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKPUBLISH_DIR="$(dirname "$SCRIPT_DIR")"
METASERVER_DIR="$WORKPUBLISH_DIR/metaServer"
DATASERVER_DIR="$WORKPUBLISH_DIR/dataServer"

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# 停止单个服务
stop_service() {
    local pid_file=$1
    local service_name=$2
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "Stopping $service_name (PID: $pid)..."
            kill "$pid"
            
            # 等待进程结束
            local attempts=0
            while ps -p "$pid" > /dev/null 2>&1 && [ $attempts -lt 10 ]; do
                sleep 1
                attempts=$((attempts + 1))
            done
            
            if ps -p "$pid" > /dev/null 2>&1; then
                log_warn "Force killing $service_name (PID: $pid)..."
                kill -9 "$pid"
            fi
            
            log_info "$service_name stopped successfully"
        else
            log_warn "$service_name was not running"
        fi
        rm -f "$pid_file"
    else
        log_warn "No PID file found for $service_name"
    fi
}

# 主函数
main() {
    echo -e "${BLUE}"
    echo "╔═══════════════════════════════════════════════════╗"
    echo "║              MinFS Cluster Shutdown              ║"
    echo "╚═══════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    log_step "Stopping MinFS cluster..."
    
    # 按相反顺序停止服务 (DataServer -> MetaServer -> etcd)
    
    # 停止 DataServers
    log_step "Stopping DataServers..."
    for i in {1..4}; do
        stop_service "$DATASERVER_DIR/pid/dataServer${i}.pid" "DataServer-${i}"
    done
    
    # 停止 MetaServers
    log_step "Stopping MetaServers..."
    for i in {1..3}; do
        stop_service "$METASERVER_DIR/pid/metaServer${i}.pid" "MetaServer-${i}"
    done
    
    # 停止 etcd
    log_step "Stopping etcd..."
    stop_service "$METASERVER_DIR/pid/etcd.pid" "etcd"
    
    echo
    log_info "✅ MinFS cluster stopped successfully!"
    echo
}

# 运行主函数
main "$@"
EOF

# 设置执行权限
chmod +x "$WORKPUBLISH_DIR/bin/start.sh"
chmod +x "$WORKPUBLISH_DIR/bin/stop.sh"

echo "=== 构建完成 ==="
echo ""
echo "workpublish 目录结构:"
echo "workpublish/"
echo "├── bin/"
echo "│   ├── start.sh          # 一键启动所有服务"
echo "│   └── stop.sh           # 一键停止所有服务"
echo "├── metaServer/"
echo "│   ├── metaServer        # MetaServer 可执行文件"
echo "│   ├── config.yaml       # 配置文件"
echo "│   ├── lib/              # 依赖库目录"
echo "│   ├── logs/             # 日志目录"
echo "│   ├── pid/              # PID 文件目录"
echo "│   └── etcd-data/        # etcd 数据目录"
echo "├── dataServer/"
echo "│   ├── dataServer        # DataServer 可执行文件" 
echo "│   ├── config.yaml       # 配置文件"
echo "│   ├── lib/              # 依赖库目录"
echo "│   ├── logs/             # 日志目录"
echo "│   └── pid/              # PID 文件目录"
echo "└── easyClient/"
echo "    └── easyClient-1.0.jar # 客户端 SDK (包含所有依赖)"
echo ""
echo "使用方法:"
echo "1. 启动集群: ./workpublish/bin/start.sh"
echo "2. 停止集群: ./workpublish/bin/stop.sh"
echo "3. 使用SDK: java -cp ./workpublish/easyClient/easyClient-1.0.jar YourMainClass"
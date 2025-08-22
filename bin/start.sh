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
WORKPUBLISH_DIR="$(dirname "$SCRIPT_DIR")/workpublish"
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

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

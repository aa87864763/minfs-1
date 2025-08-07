#!/bin/bash

echo "=== Stopping DataServer Cluster ==="

# 定义端口数组
PORTS=(8001 8002 8003 8004)

# 通过端口号强制关闭进程
for i in {0..3}; do
    PORT=${PORTS[$i]}
    SERVER_NUM=$((i + 1))
    
    echo "🛑 Checking port $PORT for DataServer $SERVER_NUM..."
    
    # 查找占用端口的进程
    PID=$(ss -tlnp | grep ":$PORT " | grep -o 'pid=[0-9]*' | cut -d'=' -f2 | head -1)
    
    if [ ! -z "$PID" ]; then
        echo "   Found process $PID listening on port $PORT"
        
        # 尝试优雅停止
        if kill -TERM $PID 2>/dev/null; then
            echo "   Sent SIGTERM to process $PID"
            sleep 3
            
            # 检查进程是否还在运行
            if kill -0 $PID 2>/dev/null; then
                echo "   Process still running, force killing..."
                kill -KILL $PID 2>/dev/null
                sleep 1
                
                if kill -0 $PID 2>/dev/null; then
                    echo "   ❌ Failed to kill process $PID"
                else
                    echo "   ✅ Process $PID force killed"
                fi
            else
                echo "   ✅ Process $PID stopped gracefully"
            fi
        else
            echo "   ❌ Failed to send signal to process $PID"
        fi
    else
        echo "   No process found on port $PORT"
    fi
done

# 额外检查：通过进程名查找遗漏的dataserver进程
echo ""
echo "🔍 Checking for any remaining dataserver processes..."
REMAINING_PIDS=$(ps aux | grep '[d]ataserver/main.go\|[m]ain.*config_dataserver' | awk '{print $2}')

if [ ! -z "$REMAINING_PIDS" ]; then
    echo "Found remaining dataserver processes: $REMAINING_PIDS"
    for PID in $REMAINING_PIDS; do
        echo "   Killing remaining process $PID..."
        kill -KILL $PID 2>/dev/null
    done
else
    echo "   No remaining dataserver processes found"
fi

# 清理文件
echo ""
echo "🧹 Cleaning up..."
rm -f dataserver*.pid
rm -f dataserver*.log

# 验证端口是否已释放
echo ""
echo "📊 Port status check:"
for PORT in ${PORTS[@]}; do
    if ss -tln | grep -q ":$PORT "; then
        echo "   ❌ Port $PORT still in use"
    else
        echo "   ✅ Port $PORT released"
    fi
done

echo ""
echo "✅ DataServer cluster shutdown complete"
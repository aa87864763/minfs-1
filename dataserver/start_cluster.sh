#!/bin/bash

echo "=== Starting 4 DataServer Cluster ==="

# 清理旧数据
rm -rf data1 data2 data3 data4
mkdir -p data1 data2 data3 data4

echo "📁 Created data directories"

# 启动4个DataServer实例（后台运行）
echo "🚀 Starting DataServer instances..."

echo "   Starting DataServer 1 on port 8001..."
go run cmd/dataserver/main.go -config=config_dataserver1.yaml -mock=false > dataserver1.log 2>&1 &
DS1_PID=$!
sleep 2

echo "   Starting DataServer 2 on port 8002..."
go run cmd/dataserver/main.go -config=config_dataserver2.yaml -mock=false > dataserver2.log 2>&1 &
DS2_PID=$!
sleep 2

echo "   Starting DataServer 3 on port 8003..."
go run cmd/dataserver/main.go -config=config_dataserver3.yaml -mock=false > dataserver3.log 2>&1 &
DS3_PID=$!
sleep 2

echo "   Starting DataServer 4 on port 8004..."
go run cmd/dataserver/main.go -config=config_dataserver4.yaml -mock=false > dataserver4.log 2>&1 &
DS4_PID=$!
sleep 2

# 保存PID到文件
echo $DS1_PID > dataserver1.pid
echo $DS2_PID > dataserver2.pid
echo $DS3_PID > dataserver3.pid
echo $DS4_PID > dataserver4.pid

echo "✅ All DataServers started"
echo "📋 Process IDs:"
echo "   DataServer 1: $DS1_PID (port 8001)"
echo "   DataServer 2: $DS2_PID (port 8002)"
echo "   DataServer 3: $DS3_PID (port 8003)"
echo "   DataServer 4: $DS4_PID (port 8004)"

echo ""
echo "📋 Commands:"
echo "   Check status: ./check_cluster.sh"
echo "   Test 3-replica: go run three_replica_test.go"
echo "   Stop cluster: ./stop_cluster.sh"
echo "   View logs: tail -f dataserver1.log"

echo ""
echo "🎯 Cluster is ready for 3-replica testing!"
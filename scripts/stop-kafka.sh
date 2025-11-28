#!/bin/bash
# Stop Kafka services

set -e

echo "🛑 Stopping Kafka services..."

if [ -f .kafka-pids ]; then
    echo ""
    echo "Reading PIDs from .kafka-pids..."
    
    PIDS=$(cat .kafka-pids)
    
    for PID in $PIDS; do
        if ps -p $PID > /dev/null 2>&1; then
            echo "  Killing process $PID"
            kill $PID
        else
            echo "  Process $PID not running"
        fi
    done
    
    rm .kafka-pids
    echo ""
    echo "✅ Kafka services stopped"
else
    echo ""
    echo "⚠️  No .kafka-pids file found. Trying to find processes..."
    
    # Try to find and kill processes by name
    pkill -f "kafka.Kafka" || echo "  No Kafka process found"
    pkill -f "zookeeper" || echo "  No Zookeeper process found"
    
    echo ""
    echo "✅ Done"
fi

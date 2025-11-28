#!/bin/bash
# Start Kafka services for local development

set -e

echo "🚀 Starting Kafka services..."
echo ""

# Check if Kafka is installed
if ! command -v kafka-server-start &> /dev/null; then
    echo "❌ Kafka not found. Install with: brew install kafka"
    exit 1
fi

# Create log directory
mkdir -p logs

echo "1️⃣  Starting Zookeeper..."
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties > logs/zookeeper.log 2>&1 &
ZOOKEEPER_PID=$!
echo "   Zookeeper PID: $ZOOKEEPER_PID"

# Wait for Zookeeper to start
sleep 5

echo "2️⃣  Starting Kafka broker..."
kafka-server-start /opt/homebrew/etc/kafka/server.properties > logs/kafka.log 2>&1 &
KAFKA_PID=$!
echo "   Kafka PID: $KAFKA_PID"

# Wait for Kafka to start
sleep 10

echo "3️⃣  Creating topics..."

# Create daily prices topic
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic tiingo.daily_prices \
  --if-not-exists

# Create crypto prices topic
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic tiingo.crypto_prices \
  --if-not-exists

echo ""
echo "✅ Kafka is ready!"
echo ""
echo "📋 Topic list:"
kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "💾 Process IDs saved to .kafka-pids"
echo "$ZOOKEEPER_PID" > .kafka-pids
echo "$KAFKA_PID" >> .kafka-pids

echo ""
echo "🛑 To stop Kafka, run: ./scripts/stop-kafka.sh"
echo "📊 View logs: tail -f logs/kafka.log"

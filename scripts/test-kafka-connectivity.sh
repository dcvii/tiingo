#!/bin/bash
# Quick connectivity test for Gold Kafka broker
# Tests network connectivity and basic Kafka operations

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

KAFKA_BROKER="gold:9092"
GOLD_HOST="gold"
GOLD_IP="192.168.1.178"

echo -e "${BLUE}🔌 Kafka Connectivity Test${NC}"
echo "=================================="
echo ""

# Test 1: DNS/Host resolution
echo -e "${YELLOW}Test 1: DNS Resolution${NC}"
if host "$GOLD_HOST" &> /dev/null; then
    RESOLVED_IP=$(host "$GOLD_HOST" | grep "has address" | awk '{print $4}')
    echo -e "${GREEN}  ✓ '$GOLD_HOST' resolves to $RESOLVED_IP${NC}"
else
    echo -e "${RED}  ✗ Cannot resolve '$GOLD_HOST'${NC}"
    exit 1
fi
echo ""

# Test 2: Ping test
echo -e "${YELLOW}Test 2: Network Reachability${NC}"
if ping -c 2 -W 2 "$GOLD_HOST" &> /dev/null; then
    LATENCY=$(ping -c 1 "$GOLD_HOST" | grep "time=" | awk -F'time=' '{print $2}' | awk '{print $1}')
    echo -e "${GREEN}  ✓ '$GOLD_HOST' is reachable (latency: ${LATENCY}ms)${NC}"
else
    echo -e "${RED}  ✗ Cannot reach '$GOLD_HOST'${NC}"
    exit 1
fi
echo ""

# Test 3: Port connectivity
echo -e "${YELLOW}Test 3: Kafka Port (9092)${NC}"
if nc -zv -w 5 "$GOLD_HOST" 9092 2>&1 | grep -q "succeeded"; then
    echo -e "${GREEN}  ✓ Port 9092 is open and accepting connections${NC}"
else
    echo -e "${RED}  ✗ Cannot connect to port 9092${NC}"
    exit 1
fi
echo ""

# Test 4: Kafka topics command (if available)
echo -e "${YELLOW}Test 4: Kafka Administrative Access${NC}"
if command -v kafka-topics &> /dev/null; then
    if timeout 10 kafka-topics --list --bootstrap-server "$KAFKA_BROKER" &> /dev/null; then
        TOPIC_COUNT=$(kafka-topics --list --bootstrap-server "$KAFKA_BROKER" 2>/dev/null | wc -l | tr -d ' ')
        echo -e "${GREEN}  ✓ Can list topics ($TOPIC_COUNT topics found)${NC}"
        
        # List topics
        echo -e "${BLUE}  Topics on $KAFKA_BROKER:${NC}"
        kafka-topics --list --bootstrap-server "$KAFKA_BROKER" 2>/dev/null | sed 's/^/    • /'
    else
        echo -e "${YELLOW}  ⚠ Cannot list topics (timeout or permission issue)${NC}"
    fi
else
    echo -e "${YELLOW}  ⚠ kafka-topics command not available${NC}"
    echo "     Install with: brew install kafka"
fi
echo ""

# Test 5: Check if tiingo topics exist
echo -e "${YELLOW}Test 5: Tiingo Topics${NC}"
if command -v kafka-topics &> /dev/null; then
    TOPICS=$(kafka-topics --list --bootstrap-server "$KAFKA_BROKER" 2>/dev/null)
    
    DAILY_TOPIC="tiingo.daily_prices"
    CRYPTO_TOPIC="tiingo.crypto_prices"
    TEST_TOPIC="tiingo.test.daily_prices"
    
    for topic in "$DAILY_TOPIC" "$CRYPTO_TOPIC" "$TEST_TOPIC"; do
        if echo "$TOPICS" | grep -q "^${topic}$"; then
            echo -e "${GREEN}  ✓ Topic '$topic' exists${NC}"
            
            # Get topic details
            PARTITIONS=$(kafka-topics --describe --bootstrap-server "$KAFKA_BROKER" --topic "$topic" 2>/dev/null | grep "PartitionCount" | awk '{print $4}')
            if [ -n "$PARTITIONS" ]; then
                echo "     Partitions: $PARTITIONS"
            fi
        else
            echo -e "${YELLOW}  ⚠ Topic '$topic' not found (will be auto-created on first use)${NC}"
        fi
    done
else
    echo -e "${YELLOW}  ⚠ Skipping (kafka-topics not available)${NC}"
fi
echo ""

# Test 6: Check consumer groups
echo -e "${YELLOW}Test 6: Consumer Groups${NC}"
if command -v kafka-consumer-groups &> /dev/null; then
    GROUPS=$(kafka-consumer-groups --list --bootstrap-server "$KAFKA_BROKER" 2>/dev/null)
    
    if [ -n "$GROUPS" ]; then
        GROUP_COUNT=$(echo "$GROUPS" | wc -l | tr -d ' ')
        echo -e "${GREEN}  ✓ Found $GROUP_COUNT consumer group(s)${NC}"
        
        # Check for tiingo groups
        if echo "$GROUPS" | grep -q "tiingo"; then
            echo -e "${BLUE}  Tiingo consumer groups:${NC}"
            echo "$GROUPS" | grep "tiingo" | sed 's/^/    • /'
        fi
    else
        echo -e "${YELLOW}  ⚠ No consumer groups found${NC}"
    fi
else
    echo -e "${YELLOW}  ⚠ Skipping (kafka-consumer-groups not available)${NC}"
fi
echo ""

# Summary
echo -e "${GREEN}✅ Connectivity Test Complete!${NC}"
echo ""
echo -e "${BLUE}📋 Configuration Summary:${NC}"
echo "  • Broker: $KAFKA_BROKER"
echo "  • IP Address: $GOLD_IP"
echo "  • Resolved IP: $RESOLVED_IP"
echo ""
echo -e "${BLUE}🚀 Next Steps:${NC}"
echo "  1. Run end-to-end test:"
echo "     ./scripts/test-kafka-e2e.sh"
echo ""
echo "  2. Start consumer (Terminal 1):"
echo "     ./consumer -brokers $KAFKA_BROKER -verbose"
echo ""
echo "  3. Run producer (Terminal 2):"
echo "     ./tiingo -brokers $KAFKA_BROKER -verbose"

#!/bin/bash
# Quick check for basic Kafka connectivity to Gold

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "Quick Kafka Check"
echo "================="
echo ""

# Check Gold connectivity
echo -n "Gold server (ping)....... "
if ping -c 1 -W 2 gold &> /dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    exit 1
fi

# Check Kafka port
echo -n "Kafka port (9092)........ "
if nc -zv -w 5 gold 9092 &> /dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    exit 1
fi

# Check API key
echo -n "TIINGO_API_KEY........... "
if [ -n "$TIINGO_API_KEY" ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC} (run: direnv allow)"
    exit 1
fi

# Check binaries
echo -n "Producer binary.......... "
if [ -f "./tiingo" ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}⚠${NC} (run: go build -o tiingo ./cmd/tiingo)"
fi

echo -n "Consumer binary.......... "
if [ -f "./consumer" ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}⚠${NC} (run: go build -o consumer ./cmd/consumer)"
fi

echo ""
echo -e "${GREEN}Ready to test!${NC}"
echo ""
echo "Next steps:"
echo "  1. Build binaries (if needed):"
echo "     go build -o tiingo ./cmd/tiingo"
echo "     go build -o consumer ./cmd/consumer"
echo ""
echo "  2. Run end-to-end test:"
echo "     ./scripts/test-kafka-e2e.sh"

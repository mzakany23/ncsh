#!/bin/bash
# Test script to verify Lambda container entrypoint configuration
# This script verifies the correct placement and permissions of bootstrap script and entrypoint symlink
# to ensure AWS Lambda will be able to properly execute the container

# Set colors for output
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
NC="\033[0m" # No Color

echo -e "${YELLOW}Starting Lambda entrypoint verification test...${NC}"

# Build the test image
echo -e "${YELLOW}Building Lambda test container...${NC}"
cd "$(dirname "$0")/../scraping"
docker build -t lambda-entrypoint-test:latest .

# Create a Dockerfile for inspection
TEMP_DIR=$(mktemp -d)
cat > "$TEMP_DIR/Dockerfile" << EOD
FROM lambda-entrypoint-test:latest
CMD ["/bin/sh", "-c", "echo 'TEST: Checking files and permissions'; \\
    ls -la /var/runtime/bootstrap || echo 'bootstrap NOT FOUND'; \\
    ls -la /lambda-entrypoint.sh || echo 'entrypoint NOT FOUND'; \\
    if [ -L /lambda-entrypoint.sh ]; then \\
        echo 'Symlink points to: '\$(readlink /lambda-entrypoint.sh); \\
    else \\
        echo 'NOT a symlink'; \\
    fi; \\
    touch /tmp/test-exec; \\
    if [ -x /var/runtime/bootstrap ]; then \\
        echo 'bootstrap is executable'; \\
    else \\
        echo 'bootstrap is NOT executable'; \\
    fi; \\
    if [ -x /lambda-entrypoint.sh ]; then \\
        echo 'entrypoint is executable'; \\
    else \\
        echo 'entrypoint is NOT executable'; \\
    fi; \\
    echo 'Testing entrypoint execution:'; \\
    /lambda-entrypoint.sh 'dummy.handler' 2>&1 || echo 'Entrypoint execution error'"]

ENTRYPOINT []
EOD

# Build and run the test container
echo -e "${YELLOW}Building and running inspection container...${NC}"
cd "$TEMP_DIR"
docker build -t lambda-entrypoint-inspector:latest .
TEST_OUTPUT=$(docker run --rm lambda-entrypoint-inspector:latest)

# Clean up temporary directory
rm -rf "$TEMP_DIR"

# Parse and display results
echo -e "${YELLOW}Analyzing test results:${NC}"
echo -e "${YELLOW}-------------------------------------------------------------------------------${NC}"
echo "$TEST_OUTPUT"
echo -e "${YELLOW}-------------------------------------------------------------------------------${NC}"

# Check for bootstrap file
if echo "$TEST_OUTPUT" | grep -q "bootstrap NOT FOUND"; then
    echo -e "${RED}✗ FAIL: Bootstrap file not found at /var/runtime/bootstrap${NC}"
    EXIT_STATUS=1
else
    echo -e "${GREEN}✓ PASS: Bootstrap file exists${NC}"
fi

# Check for entrypoint file
if echo "$TEST_OUTPUT" | grep -q "entrypoint NOT FOUND"; then
    echo -e "${RED}✗ FAIL: Lambda entrypoint not found at /lambda-entrypoint.sh${NC}"
    EXIT_STATUS=1
else
    echo -e "${GREEN}✓ PASS: Lambda entrypoint exists${NC}"
fi

# Check if entrypoint is a symlink pointing to the bootstrap
if echo "$TEST_OUTPUT" | grep -q "NOT a symlink"; then
    echo -e "${RED}✗ FAIL: /lambda-entrypoint.sh is not a symlink${NC}"
    EXIT_STATUS=1
else
    if echo "$TEST_OUTPUT" | grep -q "Symlink points to: /var/runtime/bootstrap"; then
        echo -e "${GREEN}✓ PASS: /lambda-entrypoint.sh correctly points to /var/runtime/bootstrap${NC}"
    else
        echo -e "${RED}✗ FAIL: /lambda-entrypoint.sh points to the wrong location${NC}"
        EXIT_STATUS=1
    fi
fi

# Check if bootstrap is executable
if echo "$TEST_OUTPUT" | grep -q "bootstrap is executable"; then
    echo -e "${GREEN}✓ PASS: Bootstrap file is executable${NC}"
else
    echo -e "${RED}✗ FAIL: Bootstrap file is not executable${NC}"
    EXIT_STATUS=1
fi

# Check if entrypoint is executable
if echo "$TEST_OUTPUT" | grep -q "entrypoint is executable"; then
    echo -e "${GREEN}✓ PASS: Entrypoint is executable${NC}"
else
    echo -e "${RED}✗ FAIL: Entrypoint is not executable${NC}"
    EXIT_STATUS=1
fi

# Check if entrypoint can be executed
if echo "$TEST_OUTPUT" | grep -q "Running in standard mode with handler: dummy.handler"; then
    echo -e "${GREEN}✓ PASS: Entrypoint script executes properly${NC}"
else
    echo -e "${RED}✗ FAIL: Entrypoint script did not execute properly${NC}"
    EXIT_STATUS=1
fi

# Clean up images
echo -e "${YELLOW}Cleaning up test containers...${NC}"
docker rmi lambda-entrypoint-inspector:latest >/dev/null 2>&1

# Final results
if [ -z "$EXIT_STATUS" ]; then
    echo -e "${GREEN}All tests PASSED! Lambda container entrypoint is correctly configured.${NC}"
    exit 0
else
    echo -e "${RED}Some tests FAILED. The Lambda container entrypoint needs to be fixed.${NC}"
    exit 1
fi

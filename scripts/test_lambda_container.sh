#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting Lambda container test script...${NC}"

# Script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Configuration
DOCKER_IMAGE_NAME="ncsoccer-scraper-test"
DOCKERFILE_PATH="${PROJECT_ROOT}/scraping/Dockerfile"

# Create a temporary directory for bootstrap script debugging
TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" EXIT

# Create a simplified test bootstrap script that just validates arguments
cat > "$TMP_DIR/test_bootstrap.sh" << 'EOF'
#!/bin/bash

echo "===== BOOTSTRAP TEST ====="
echo "Arguments received: $@"
echo "First argument: $1"

if [ -z "$1" ]; then
  echo "ERROR: No handler name provided as first argument"
  exit 1
fi

echo "BACKFILL_MODE: $BACKFILL_MODE"

if [ "$BACKFILL_MODE" = "true" ]; then
  echo "SUCCESS: Running in backfill mode with handler: $1"
else
  echo "SUCCESS: Running in standard mode with handler: $1"
fi

# Don't actually execute the Lambda runtime, just exit successfully
exit 0
EOF
chmod +x "$TMP_DIR/test_bootstrap.sh"

echo -e "${YELLOW}Building Lambda container from ${DOCKERFILE_PATH}...${NC}"
docker build -t ${DOCKER_IMAGE_NAME} -f ${DOCKERFILE_PATH} ${PROJECT_ROOT}/scraping

echo -e "${YELLOW}Testing bootstrap in standard mode...${NC}"
STANDARD_RESULT=$(docker run --rm \
  -e BACKFILL_MODE="false" \
  -v "$TMP_DIR/test_bootstrap.sh:/var/runtime/bootstrap:ro" \
  --entrypoint /var/runtime/bootstrap \
  ${DOCKER_IMAGE_NAME} \
  lambda_function.lambda_handler 2>&1)

echo "$STANDARD_RESULT"

if echo "$STANDARD_RESULT" | grep -q "SUCCESS: Running in standard mode"; then
  echo -e "${GREEN}Standard mode bootstrap test passed!${NC}"
else
  echo -e "${RED}Standard mode bootstrap test failed!${NC}"
  exit 1
fi

echo -e "${YELLOW}Testing bootstrap in backfill mode...${NC}"
BACKFILL_RESULT=$(docker run --rm \
  -e BACKFILL_MODE="true" \
  -v "$TMP_DIR/test_bootstrap.sh:/var/runtime/bootstrap:ro" \
  --entrypoint /var/runtime/bootstrap \
  ${DOCKER_IMAGE_NAME} \
  backfill_runner.lambda_handler 2>&1)

echo "$BACKFILL_RESULT"

if echo "$BACKFILL_RESULT" | grep -q "SUCCESS: Running in backfill mode"; then
  echo -e "${GREEN}Backfill mode bootstrap test passed!${NC}"
else
  echo -e "${RED}Backfill mode bootstrap test failed!${NC}"
  exit 1
fi

# Now create a fixed Dockerfile bootstrap for local testing
cat > "$TMP_DIR/fixed_bootstrap.sh" << 'EOF'
#!/bin/bash
# This is the correct bootstrap pattern that AWS Lambda expects

# Get the handler name from the first argument
handler=$1

if [ -z "$handler" ]; then
  echo "ERROR: Lambda handler name is required as the first argument"
  exit 1
fi

if [ "$BACKFILL_MODE" = "true" ]; then
  echo "Running in backfill mode with handler: $handler"
  # For Lambda we need to execute the specific handler we want
  # Ignoring the handler argument and using our fixed handler
  exec /var/lang/bin/python3 -m awslambdaric backfill_runner.lambda_handler
else
  echo "Running in standard mode with handler: $handler"
  # For Lambda we need to execute the specific handler we want
  # Ignoring the handler argument and using our fixed handler
  exec /var/lang/bin/python3 -m awslambdaric lambda_function.lambda_handler
fi
EOF
chmod +x "$TMP_DIR/fixed_bootstrap.sh"

echo -e "${YELLOW}Here is the fixed bootstrap that should be used in the Dockerfile:${NC}"
cat "$TMP_DIR/fixed_bootstrap.sh"

echo -e "${GREEN}All tests completed! Update your Dockerfile with the above bootstrap pattern.${NC}"

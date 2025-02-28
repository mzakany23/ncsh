#!/bin/bash
# Soccer Query System Smoke Test
# Run this script to test the query system with a series of questions and follow-ups

# Text formatting
BOLD='\033[1m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Create a unique session ID for this test run
SESSION_ID="smoke_test_$(date +%Y%m%d_%H%M%S)"
export QUERY_SESSION=$SESSION_ID

echo -e "${BOLD}Soccer Query System Smoke Test${NC}"
echo -e "Session ID: ${BLUE}$SESSION_ID${NC}"
echo -e "Database: ${BLUE}analysis/matches.parquet${NC}"
echo "=============================================="

run_query() {
    query="$1"
    echo -e "\n${BOLD}${YELLOW}QUERY:${NC} $query"
    echo -e "${BOLD}${GREEN}RESPONSE:${NC}"
    # Save the current directory
    CURRENT_DIR=$(pwd)
    # Change to the root directory and run the command
    cd $(dirname $CURRENT_DIR)
    make query-llama query="$query" session_id=$SESSION_ID verbose=true
    # Return to the original directory
    cd $CURRENT_DIR
    echo "--------------------------------------------"
    sleep 1
}

echo -e "\n${BOLD}${BLUE}Basic Queries${NC}"
echo "=============================================="

# Test 1: Basic team query
run_query "How is key west doing these days?"

# Test 2: Follow-up query
run_query "ok, how is their win/loss percentage this month"

echo -e "\n${BOLD}${BLUE}Error Handling${NC}"
echo "=============================================="
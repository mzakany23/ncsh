#!/usr/bin/env python3
"""
Test script to simulate running the Lambda function locally.
This will help diagnose S3 write and timeout issues.
"""

import os
import sys
import json
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("lambda_local_test.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("lambda_test")

# Import the lambda function
from lambda_function import lambda_handler

def simulate_lambda_context():
    """Create a simple object to simulate Lambda context"""
    class MockContext:
        def __init__(self):
            self.function_name = "ncsoccer_scraper_local_test"
            self.memory_limit_in_mb = 512
            self.invoked_function_arn = "arn:aws:lambda:us-east-2:MOCK:function:ncsoccer_scraper_local_test"
            self.aws_request_id = "mock-request-id"

        def get_remaining_time_in_millis(self):
            # Simulate a Lambda with 15 minutes of runtime
            return 15 * 60 * 1000

    return MockContext()

def main():
    logger.info("Starting local Lambda function test")

    # Simulate the same event that would be passed by the Step Function
    event = {
        "mode": "day",
        "parameters": {
            "year": datetime.now().year,
            "month": datetime.now().month,
            "day": datetime.now().day,
            "force_scrape": True,
            "architecture_version": "v2"
        }
    }

    logger.info(f"Using event: {json.dumps(event)}")

    start_time = time.time()

    try:
        context = simulate_lambda_context()
        result = lambda_handler(event, context)

        logger.info(f"Lambda execution completed in {time.time() - start_time:.2f} seconds")
        logger.info(f"Result: {json.dumps(result)}")
        return result
    except Exception as e:
        logger.error(f"Error during Lambda execution: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

if __name__ == "__main__":
    main()
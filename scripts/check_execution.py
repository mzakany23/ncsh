#!/usr/bin/env python3

import argparse
import boto3
import json
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_execution_status(execution_arn):
    """
    Get the current status of a Step Function execution.

    Args:
        execution_arn (str): The ARN of the execution to check

    Returns:
        dict: The execution details including status, input, output, etc.
    """
    try:
        client = boto3.client('stepfunctions')
        response = client.describe_execution(
            executionArn=execution_arn
        )
        return response
    except Exception as e:
        logger.error(f"Failed to get execution status: {str(e)}")
        raise

def format_duration(start_time, stop_time=None):
    """Calculate and format the duration of an execution."""
    if not stop_time:
        stop_time = datetime.now()
    duration = stop_time - start_time
    total_seconds = duration.total_seconds()
    minutes = int(total_seconds // 60)
    seconds = int(total_seconds % 60)
    return f"{minutes}m {seconds}s"

def wait_for_completion(execution_arn, check_interval=10, timeout=3600):
    """
    Wait for a Step Function execution to complete.

    Args:
        execution_arn (str): The ARN of the execution to monitor
        check_interval (int): How often to check status in seconds
        timeout (int): Maximum time to wait in seconds

    Returns:
        dict: The final execution status
    """
    start_time = datetime.now()
    elapsed = 0

    while elapsed < timeout:
        status = get_execution_status(execution_arn)
        current_status = status['status']

        # Format duration
        duration = format_duration(start_time)

        if current_status in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
            logger.info(f"Execution finished with status: {current_status}")
            if current_status == 'SUCCEEDED':
                try:
                    output = json.loads(status.get('output', '{}'))
                    logger.info(f"Execution output: {json.dumps(output, indent=2)}")
                except json.JSONDecodeError:
                    logger.warning("Could not parse execution output as JSON")
            elif current_status == 'FAILED':
                error = status.get('error')
                cause = status.get('cause')
                logger.error(f"Execution failed with error: {error}")
                if cause:
                    logger.error(f"Cause: {cause}")
            return status

        logger.info(f"Current status: {current_status} (Running for: {duration})")
        time.sleep(check_interval)
        elapsed = (datetime.now() - start_time).total_seconds()

    raise TimeoutError(f"Execution did not complete within {timeout} seconds")

def main():
    parser = argparse.ArgumentParser(description='Check NC Soccer Scraper Step Function Execution Status')
    parser.add_argument('--execution-arn', required=True,
                      help='The ARN of the execution to check')
    parser.add_argument('--wait', action='store_true',
                      help='Wait for execution to complete')
    parser.add_argument('--check-interval', type=int, default=10,
                      help='How often to check status when waiting (seconds)')
    parser.add_argument('--timeout', type=int, default=3600,
                      help='Maximum time to wait for completion (seconds)')
    parser.add_argument('--profile', help='AWS profile to use')
    parser.add_argument('--region', default='us-east-2',
                      help='AWS region (default: us-east-2)')

    args = parser.parse_args()

    # Configure AWS session
    if args.profile:
        boto3.setup_default_session(profile_name=args.profile, region_name=args.region)
    else:
        boto3.setup_default_session(region_name=args.region)

    try:
        if args.wait:
            status = wait_for_completion(
                args.execution_arn,
                check_interval=args.check_interval,
                timeout=args.timeout
            )
        else:
            status = get_execution_status(args.execution_arn)
            logger.info(f"Current status: {status['status']}")
            try:
                if status.get('output'):
                    output = json.loads(status['output'])
                    logger.info(f"Execution output: {json.dumps(output, indent=2)}")
            except json.JSONDecodeError:
                logger.warning("Could not parse execution output as JSON")

        # Exit with appropriate status code
        if status['status'] == 'SUCCEEDED':
            exit(0)
        else:
            exit(1)

    except Exception as e:
        logger.error(f"Error checking execution status: {str(e)}")
        exit(1)

if __name__ == '__main__':
    main()
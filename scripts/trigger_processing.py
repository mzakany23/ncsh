#!/usr/bin/env python3

import argparse
import boto3
import json
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def trigger_processing(state_machine_arn, src_bucket=None, src_prefix=None, dst_bucket=None, dst_prefix=None):
    """
    Trigger the processing state machine to convert JSON to Parquet.

    Args:
        state_machine_arn (str): The ARN of the state machine to execute
        src_bucket (str, optional): Source S3 bucket
        src_prefix (str, optional): Source prefix for JSON files
        dst_bucket (str, optional): Destination S3 bucket
        dst_prefix (str, optional): Destination prefix for Parquet files

    Returns:
        dict: The response from the start_execution API call
    """
    try:
        client = boto3.client('stepfunctions')

        # Build input based on provided parameters with defaults
        input_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "src_bucket": src_bucket or os.environ.get("DATA_BUCKET", "ncsh-app-data"),
            "src_prefix": src_prefix or os.environ.get("JSON_PREFIX", "data/json/"),
            "dst_bucket": dst_bucket or os.environ.get("DATA_BUCKET", "ncsh-app-data"),
            "dst_prefix": dst_prefix or os.environ.get("PARQUET_PREFIX", "data/parquet/")
        }

        # Start the execution
        response = client.start_execution(
            stateMachineArn=state_machine_arn,
            name=f"processing-{datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')}",
            input=json.dumps(input_data)
        )

        logger.info(f"Started execution: {response['executionArn']}")
        print(f"Execution ARN: {response['executionArn']}")
        return response

    except Exception as e:
        logger.error(f"Failed to start execution: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Trigger NC Soccer Processing State Machine')
    parser.add_argument('--state-machine-arn', required=True,
                      help='The ARN of the state machine to execute')
    parser.add_argument('--src-bucket',
                      help='Source S3 bucket (optional)')
    parser.add_argument('--src-prefix',
                      help='Source prefix for JSON files (optional)')
    parser.add_argument('--dst-bucket',
                      help='Destination S3 bucket (optional)')
    parser.add_argument('--dst-prefix',
                      help='Destination prefix for Parquet files (optional)')
    parser.add_argument('--profile',
                      help='AWS profile to use')
    parser.add_argument('--region', default='us-east-2',
                      help='AWS region (default: us-east-2)')

    args = parser.parse_args()

    # Configure AWS session
    if args.profile:
        boto3.setup_default_session(profile_name=args.profile, region_name=args.region)
    else:
        boto3.setup_default_session(region_name=args.region)

    try:
        trigger_processing(
            args.state_machine_arn,
            src_bucket=args.src_bucket,
            src_prefix=args.src_prefix,
            dst_bucket=args.dst_bucket,
            dst_prefix=args.dst_prefix
        )
    except Exception as e:
        logger.error(f"Error triggering processing: {str(e)}")
        exit(1)

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
"""
Daily Backfill Script

This script triggers the recursive workflow for the last 3 days to ensure complete data.
It can be run manually or scheduled via cron/EventBridge.
"""

import os
import sys
import json
import logging
import argparse
import boto3
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def trigger_step_function(state_machine_arn, input_data):
    """
    Trigger a Step Function execution with the given input data.
    
    Args:
        state_machine_arn (str): ARN of the Step Function state machine
        input_data (dict): Input data for the Step Function
        
    Returns:
        dict: Response from the start_execution API call
    """
    client = boto3.client('stepfunctions')
    execution_name = f"daily-backfill-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    
    response = client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(input_data)
    )
    
    return response

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Trigger daily backfill for the recursive workflow')
    
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--days', type=int, default=3, help='Number of days to backfill (default: 3)')
    parser.add_argument('--force-scrape', action='store_true', help='Force scrape even if data exists')
    parser.add_argument('--batch-size', type=int, default=1, help='Batch size for processing')
    parser.add_argument('--bucket', default='ncsh-app-data', help='S3 bucket name')
    parser.add_argument('--architecture-version', default='v2', help='Architecture version')
    
    args = parser.parse_args()
    
    # Configure AWS region
    boto3.setup_default_session(region_name=args.region)
    
    # State machine ARN for the recursive workflow
    state_machine_arn = "arn:aws:states:us-east-2:552336166511:stateMachine:ncsoccer-unified-workflow-recursive"
    
    # Calculate date range for the last N days
    today = datetime.now()
    end_date = today.strftime('%Y-%m-%d')
    start_date = (today - timedelta(days=args.days-1)).strftime('%Y-%m-%d')
    
    logger.info(f"Backfilling data for the last {args.days} days: {start_date} to {end_date}")
    
    # Prepare input for the recursive workflow
    input_data = {
        "start_date": start_date,
        "end_date": end_date,
        "force_scrape": args.force_scrape,
        "batch_size": args.batch_size,
        "bucket_name": args.bucket,
        "architecture_version": args.architecture_version,
        "is_sub_execution": False
    }
    
    logger.info(f"Input data: {json.dumps(input_data, indent=2)}")
    
    try:
        # Trigger the step function
        response = trigger_step_function(state_machine_arn, input_data)
        logger.info(f"Step Function execution started successfully")
        
        # Print link to AWS console
        console_url = f"https://{args.region}.console.aws.amazon.com/states/home?region={args.region}#/executions/details/{response['executionArn']}"
        logger.info(f"Console URL: {console_url}")
    except Exception as e:
        logger.error(f"Failed to trigger Step Function: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

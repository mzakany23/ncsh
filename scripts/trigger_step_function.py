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

def trigger_step_function(state_machine_arn, input_data):
    """
    Trigger an AWS Step Function execution.

    Args:
        state_machine_arn (str): The ARN of the state machine to execute
        input_data (dict): The input data for the state machine

    Returns:
        dict: The response from the start_execution API call
    """
    try:
        client = boto3.client('stepfunctions')
        response = client.start_execution(
            stateMachineArn=state_machine_arn,
            name=f"scraper-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}",
            input=json.dumps(input_data)
        )
        logger.info(f"Started execution: {response['executionArn']}")
        return response
    except Exception as e:
        logger.error(f"Failed to start execution: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Trigger NC Soccer Scraper Step Function')
    parser.add_argument('--state-machine-arn', required=True,
                      help='The ARN of the state machine to execute')
    parser.add_argument('--year', type=int, default=datetime.now().year,
                      help='Year to scrape (default: current year)')
    parser.add_argument('--month', type=int, default=datetime.now().month,
                      help='Month to scrape (default: current month)')
    parser.add_argument('--day', type=int,
                      help='Day to scrape (if not provided, will scrape entire month)')
    parser.add_argument('--mode', choices=['day', 'month'], default='month',
                      help='Scraping mode (default: month)')
    parser.add_argument('--profile', help='AWS profile to use')
    parser.add_argument('--region', default='us-east-2',
                      help='AWS region (default: us-east-2)')

    args = parser.parse_args()

    # Configure AWS session
    if args.profile:
        boto3.setup_default_session(profile_name=args.profile, region_name=args.region)
    else:
        boto3.setup_default_session(region_name=args.region)

    # Prepare input data
    input_data = {
        "year": args.year,
        "month": args.month,
        "mode": args.mode
    }
    if args.day:
        input_data["day"] = args.day

    try:
        # Trigger the step function
        response = trigger_step_function(args.state_machine_arn, input_data)
        logger.info(f"Step Function execution started successfully")
        logger.info(f"Execution ARN: {response['executionArn']}")
        logger.info(f"Started at: {response['startDate']}")
    except Exception as e:
        logger.error(f"Failed to trigger Step Function: {str(e)}")
        exit(1)

if __name__ == '__main__':
    main()
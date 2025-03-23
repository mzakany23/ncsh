#!/usr/bin/env python3

import argparse
import boto3
import json
import logging
from datetime import datetime
import os
import calendar

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
    """
    try:
        client = boto3.client('stepfunctions')
        execution_name = f"scraper-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"

        response = client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(input_data)
        )

        logger.info(f"Started execution: {response['executionArn']}")
        logger.info(f"Execution name: {execution_name}")
        logger.info(f"Start time: {response['startDate']}")

        return response
    except Exception as e:
        logger.error(f"Failed to start execution: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Trigger NC Soccer Scraper Batched Unified Workflow')
    parser.add_argument('--state-machine-arn',
                      help='The ARN of the state machine to execute')

    # Date selection options
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument('--date', help='Single date to scrape (format: YYYY-MM-DD)')
    date_group.add_argument('--date-range', nargs=2, metavar=('START_DATE', 'END_DATE'),
                           help='Date range to scrape (format: YYYY-MM-DD YYYY-MM-DD)')
    date_group.add_argument('--month', nargs=2, metavar=('YEAR', 'MONTH'),
                           help='Full month to scrape (format: YYYY MM)')

    # Additional options
    parser.add_argument('--force-scrape', action='store_true',
                      help='Force re-scraping even if data was already scraped')
    parser.add_argument('--batch-size', type=int, default=3,
                      help='Number of days per batch (default: 3)')
    parser.add_argument('--profile', help='AWS profile to use')
    parser.add_argument('--region', default='us-east-2',
                      help='AWS region (default: us-east-2)')
    parser.add_argument('--bucket', default='ncsh-app-data',
                      help='S3 bucket name (default: ncsh-app-data)')
    parser.add_argument('--architecture-version', default='v2',
                      help='Architecture version to use (default: v2)')

    args = parser.parse_args()

    # Configure AWS session
    if args.profile:
        boto3.setup_default_session(profile_name=args.profile, region_name=args.region)
    else:
        boto3.setup_default_session(region_name=args.region)

    # If state machine ARN was not provided, use the default unified workflow
    state_machine_arn = args.state_machine_arn
    if not state_machine_arn:
        state_machine_arn = f"arn:aws:states:{args.region}:552336166511:stateMachine:ncsoccer-unified-workflow-batched"
        logger.info(f"Using default state machine ARN: {state_machine_arn}")

    # Determine start and end dates based on input
    if args.date:
        start_date = args.date
        end_date = args.date
        logger.info(f"Single date mode: {start_date}")
    elif args.date_range:
        start_date, end_date = args.date_range
        logger.info(f"Date range mode: {start_date} to {end_date}")
    else:  # args.month
        year, month = int(args.month[0]), int(args.month[1])
        _, last_day = calendar.monthrange(year, month)
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-{last_day:02d}"
        logger.info(f"Month mode: {year}-{month:02d} ({start_date} to {end_date})")

    # Prepare input for the unified workflow
    input_data = {
        "start_date": start_date,
        "end_date": end_date,
        "force_scrape": args.force_scrape,
        "batch_size": args.batch_size,
        "bucket_name": args.bucket,
        "architecture_version": args.architecture_version
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
        exit(1)

if __name__ == '__main__':
    main()
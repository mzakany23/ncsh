#!/usr/bin/env python3
"""
Setup scheduled jobs in AWS EventBridge (CloudWatch Events) to trigger Step Functions directly in the cloud.
This eliminates the need for local cron jobs by having AWS handle the scheduling.
"""

import argparse
import boto3
import json
import logging
import sys
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def setup_daily_scrape_rule(aws_profile, aws_account, region, hour, minute, force_scrape=False):
    """
    Set up an EventBridge rule to trigger the scraper Step Function daily.

    Args:
        aws_profile: AWS profile to use
        aws_account: AWS account number
        region: AWS region
        hour: Hour for daily execution (0-23)
        minute: Minute for daily execution (0-59)
        force_scrape: Whether to force re-scraping

    Returns:
        The ARN of the created rule if successful, None otherwise
    """
    try:
        # Create boto3 session with the specified profile
        session = boto3.Session(profile_name=aws_profile, region_name=region)
        events_client = session.client('events')

        # Create a rule name with timestamp to ensure uniqueness
        rule_name = f"ncsoccer-daily-scrape-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Create the scheduling expression (cron format)
        # Format: cron(minute hour day-of-month month day-of-week year)
        # For daily at specific time: cron(minute hour * * ? *)
        cron_expression = f"cron({minute} {hour} * * ? *)"

        # Create the rule
        response = events_client.put_rule(
            Name=rule_name,
            ScheduleExpression=cron_expression,
            State='ENABLED',
            Description=f'Daily NC Soccer scraper at {hour:02d}:{minute:02d} UTC',
        )

        rule_arn = response['RuleArn']
        logger.info(f"Created EventBridge rule: {rule_name} with ARN: {rule_arn}")

        # Prepare the input for the Step Function
        sf_input = {
            "year": "${aws:DateNow(YYYY)}",
            "month": "${aws:DateNow(MM)}",
            "mode": "day",
            "day": "${aws:DateNow(DD)}"
        }

        # Add force_scrape flag if specified
        if force_scrape:
            sf_input["force_scrape"] = True

        # Get the Step Function ARN
        sf_arn = f"arn:aws:states:{region}:{aws_account}:stateMachine:ncsoccer-workflow"

        # Create the target (the Step Function to invoke)
        response = events_client.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',  # Target ID must be unique within the rule
                    'Arn': sf_arn,
                    'RoleArn': f"arn:aws:iam::{aws_account}:role/EventBridgeStepFunctionExecutionRole",
                    'Input': json.dumps(sf_input)
                }
            ]
        )

        failed_entries = response.get('FailedEntryCount', 0)
        if failed_entries > 0:
            logger.error(f"Failed to create target: {response.get('FailedEntries')}")
            return None

        logger.info(f"Successfully set up EventBridge rule {rule_name} to trigger Step Function {sf_arn} daily at {hour:02d}:{minute:02d} UTC")
        logger.info(f"Input that will be passed to the Step Function: {json.dumps(sf_input, indent=2)}")

        # Set up a second rule for processing (runs 30 minutes after scraping)
        process_rule_name = f"ncsoccer-daily-process-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Calculate time for processing (30 minutes after scraping)
        process_minute = (minute + 30) % 60
        process_hour = (hour + ((minute + 30) // 60)) % 24

        process_cron_expression = f"cron({process_minute} {process_hour} * * ? *)"

        # Create the processing rule
        response = events_client.put_rule(
            Name=process_rule_name,
            ScheduleExpression=process_cron_expression,
            State='ENABLED',
            Description=f'Daily NC Soccer processor at {process_hour:02d}:{process_minute:02d} UTC',
        )

        process_rule_arn = response['RuleArn']
        logger.info(f"Created EventBridge rule: {process_rule_name} with ARN: {process_rule_arn}")

        # Get the Processing Step Function ARN
        process_sf_arn = f"arn:aws:states:{region}:{aws_account}:stateMachine:ncsoccer-processing"

        # Create the target for processing
        process_input = {
            "timestamp": "#{$.execution_time}",
            "src_bucket": "ncsh-app-data",
            "src_prefix": "data/json/",
            "dst_bucket": "ncsh-app-data",
            "dst_prefix": "data/parquet/"
        }

        response = events_client.put_targets(
            Rule=process_rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': process_sf_arn,
                    'RoleArn': f"arn:aws:iam::{aws_account}:role/EventBridgeStepFunctionExecutionRole",
                    'Input': json.dumps(process_input)
                }
            ]
        )

        failed_entries = response.get('FailedEntryCount', 0)
        if failed_entries > 0:
            logger.error(f"Failed to create processing target: {response.get('FailedEntries')}")
            return None

        logger.info(f"Successfully set up EventBridge rule {process_rule_name} to trigger processing Step Function {process_sf_arn} daily at {process_hour:02d}:{process_minute:02d} UTC")

        return rule_arn

    except Exception as e:
        logger.error(f"Error setting up daily scrape rule: {str(e)}")
        return None

def setup_first_of_month_rule(aws_profile, aws_account, region, hour, minute, force_scrape=False):
    """
    Set up an EventBridge rule to trigger scraping the entire month on the first day of each month.

    Args:
        aws_profile: AWS profile to use
        aws_account: AWS account number
        region: AWS region
        hour: Hour for execution (0-23)
        minute: Minute for execution (0-59)
        force_scrape: Whether to force re-scraping

    Returns:
        The ARN of the created rule if successful, None otherwise
    """
    try:
        # Create boto3 session with the specified profile
        session = boto3.Session(profile_name=aws_profile, region_name=region)
        events_client = session.client('events')

        # Create a rule name with timestamp to ensure uniqueness
        rule_name = f"ncsoccer-monthly-scrape-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Create the scheduling expression (cron format)
        # Format: cron(minute hour day-of-month month day-of-week year)
        # For first day of each month: cron(minute hour 1 * ? *)
        cron_expression = f"cron({minute} {hour} 1 * ? *)"

        # Create the rule
        response = events_client.put_rule(
            Name=rule_name,
            ScheduleExpression=cron_expression,
            State='ENABLED',
            Description=f'Monthly NC Soccer scraper (entire month) at {hour:02d}:{minute:02d} UTC on the 1st day',
        )

        rule_arn = response['RuleArn']
        logger.info(f"Created EventBridge rule: {rule_name} with ARN: {rule_arn}")

        # Prepare the input for the Step Function - this time with month mode
        sf_input = {
            "year": "${aws:DateNow(YYYY)}",
            "month": "${aws:DateNow(MM)}",
            "mode": "month"  # This will scrape the entire month
        }

        # Add force_scrape flag if specified
        if force_scrape:
            sf_input["force_scrape"] = True

        # Get the Step Function ARN
        sf_arn = f"arn:aws:states:{region}:{aws_account}:stateMachine:ncsoccer-workflow"

        # Create the target (the Step Function to invoke)
        response = events_client.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',  # Target ID must be unique within the rule
                    'Arn': sf_arn,
                    'RoleArn': f"arn:aws:iam::{aws_account}:role/EventBridgeStepFunctionExecutionRole",
                    'Input': json.dumps(sf_input)
                }
            ]
        )

        failed_entries = response.get('FailedEntryCount', 0)
        if failed_entries > 0:
            logger.error(f"Failed to create target: {response.get('FailedEntries')}")
            return None

        logger.info(f"Successfully set up EventBridge rule {rule_name} to trigger Step Function {sf_arn} on the 1st day of each month at {hour:02d}:{minute:02d} UTC")
        logger.info(f"Input that will be passed to the Step Function: {json.dumps(sf_input, indent=2)}")

        # Set up a rule for processing (runs 2 hours after scraping the month)
        process_rule_name = f"ncsoccer-monthly-process-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Calculate time for processing (2 hours after scraping)
        process_minute = minute
        process_hour = (hour + 2) % 24

        process_cron_expression = f"cron({process_minute} {process_hour} 1 * ? *)"

        # Create the processing rule
        response = events_client.put_rule(
            Name=process_rule_name,
            ScheduleExpression=process_cron_expression,
            State='ENABLED',
            Description=f'Monthly NC Soccer processor at {process_hour:02d}:{process_minute:02d} UTC on the 1st day',
        )

        process_rule_arn = response['RuleArn']
        logger.info(f"Created EventBridge rule: {process_rule_name} with ARN: {process_rule_arn}")

        # Get the Processing Step Function ARN
        process_sf_arn = f"arn:aws:states:{region}:{aws_account}:stateMachine:ncsoccer-processing"

        # Create the target for processing
        process_input = {
            "timestamp": "#{$.execution_time}",
            "src_bucket": "ncsh-app-data",
            "src_prefix": "data/json/",
            "dst_bucket": "ncsh-app-data",
            "dst_prefix": "data/parquet/"
        }

        response = events_client.put_targets(
            Rule=process_rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': process_sf_arn,
                    'RoleArn': f"arn:aws:iam::{aws_account}:role/EventBridgeStepFunctionExecutionRole",
                    'Input': json.dumps(process_input)
                }
            ]
        )

        failed_entries = response.get('FailedEntryCount', 0)
        if failed_entries > 0:
            logger.error(f"Failed to create processing target: {response.get('FailedEntries')}")
            return None

        logger.info(f"Successfully set up EventBridge rule {process_rule_name} to trigger processing Step Function {process_sf_arn} on the 1st day of each month at {process_hour:02d}:{process_minute:02d} UTC")

        return rule_arn

    except Exception as e:
        logger.error(f"Error setting up first-of-month rule: {str(e)}")
        return None

def parse_time(time_str):
    """Parse a time string in HH:MM format into hours and minutes."""
    try:
        hour, minute = map(int, time_str.split(':'))
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            raise ValueError("Invalid time range")
        return hour, minute
    except Exception:
        raise ValueError(f"Invalid time format: {time_str}. Use HH:MM format (24-hour).")

def main():
    parser = argparse.ArgumentParser(description='Setup AWS EventBridge scheduled rules for NC Soccer data scraping')
    parser.add_argument('--daily-time-utc', type=str, default='04:00',
                        help='Time for daily scrape in 24-hour format UTC (default: 04:00)')
    parser.add_argument('--monthly-time-utc', type=str, default='05:00',
                        help='Time for monthly scrape on 1st day in 24-hour format UTC (default: 05:00)')
    parser.add_argument('--aws-profile', default='mzakany',
                        help='AWS profile to use (default: mzakany)')
    parser.add_argument('--aws-account', default='552336166511',
                        help='AWS account number (default: 552336166511)')
    parser.add_argument('--region', default='us-east-2',
                        help='AWS region (default: us-east-2)')
    parser.add_argument('--force-scrape', action='store_true',
                        help='Force re-scrape even if already scraped')
    parser.add_argument('--skip-daily', action='store_true',
                        help='Skip setting up the daily scrape job')
    parser.add_argument('--skip-monthly', action='store_true',
                        help='Skip setting up the monthly scrape job')

    args = parser.parse_args()

    # Track success for each rule
    daily_rule_arn = None
    monthly_rule_arn = None

    # Setup daily scrape rule if not skipped
    if not args.skip_daily:
        try:
            hour, minute = parse_time(args.daily_time_utc)
            logger.info(f"Setting up daily scrape rule at {hour:02d}:{minute:02d} UTC")

            daily_rule_arn = setup_daily_scrape_rule(
                args.aws_profile,
                args.aws_account,
                args.region,
                hour,
                minute,
                args.force_scrape
            )

            if not daily_rule_arn:
                logger.error("Failed to setup daily scrape rule")

        except ValueError as e:
            logger.error(f"Error with daily time: {str(e)}")

    # Setup monthly scrape rule if not skipped
    if not args.skip_monthly:
        try:
            hour, minute = parse_time(args.monthly_time_utc)
            logger.info(f"Setting up monthly scrape rule at {hour:02d}:{minute:02d} UTC on the 1st day of each month")

            monthly_rule_arn = setup_first_of_month_rule(
                args.aws_profile,
                args.aws_account,
                args.region,
                hour,
                minute,
                args.force_scrape
            )

            if not monthly_rule_arn:
                logger.error("Failed to setup monthly scrape rule")

        except ValueError as e:
            logger.error(f"Error with monthly time: {str(e)}")

    # Check if any rules were created successfully
    if daily_rule_arn or monthly_rule_arn:
        logger.info("Successfully setup cloud-based scheduled job(s)")

        if daily_rule_arn:
            logger.info("Daily scrape rule created successfully")

        if monthly_rule_arn:
            logger.info("Monthly scrape rule created successfully")

        logger.info("\nIMPORTANT: Make sure the IAM role 'EventBridgeStepFunctionExecutionRole' exists")
        logger.info("and has permissions to invoke Step Functions. If not, create it with the following policy:")
        logger.info("""
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "states:StartExecution"
                    ],
                    "Resource": [
                        "arn:aws:states:*:*:stateMachine:ncsoccer-*"
                    ]
                }
            ]
        }
        """)
        return 0
    else:
        logger.error("No cloud-based scheduled jobs were created")
        return 1

if __name__ == '__main__':
    sys.exit(main())
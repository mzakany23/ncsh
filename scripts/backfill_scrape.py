#!/usr/bin/env python3
"""
Backfill script to scrape and process data for all months from 2007 to 2025.
"""

import argparse
import subprocess
import logging
import time
from datetime import datetime, timedelta
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("backfill.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_command(command):
    """Run a shell command and return the output."""
    logger.info(f"Running command: {command}")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        logger.info(f"Command succeeded with output: {result.stdout}")
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: {e.stderr}")
        return False, e.stderr

def check_execution_status(execution_arn, max_attempts=30, sleep_seconds=10):
    """Check the status of a Step Function execution until it completes or fails."""
    logger.info(f"Checking execution status for: {execution_arn}")

    for attempt in range(max_attempts):
        command = f"AWS_PROFILE=mzakany python scripts/check_execution.py --execution-arn {execution_arn}"
        success, output = run_command(command)

        if not success:
            logger.error(f"Failed to check execution status: {output}")
            return False

        if "SUCCEEDED" in output:
            logger.info(f"Execution completed successfully: {execution_arn}")
            return True
        elif "FAILED" in output or "TIMED_OUT" in output or "ABORTED" in output:
            logger.error(f"Execution failed: {execution_arn}")
            return False

        logger.info(f"Execution still running ({attempt+1}/{max_attempts}), waiting {sleep_seconds} seconds...")
        time.sleep(sleep_seconds)

    logger.error(f"Timed out waiting for execution to complete: {execution_arn}")
    return False

def extract_execution_arn(output):
    """Extract the execution ARN from the command output."""
    import re
    match = re.search(r'Execution ARN: (arn:aws:states:[\w\-:]+)', output)
    if match:
        return match.group(1)
    return None

def scrape_and_process_month(year, month, aws_account, force_scrape=False, wait_for_completion=True):
    """Scrape a specific month and process the data afterwards."""
    logger.info(f"Starting scrape for {year}-{month:02d}")

    # Build the scrape command
    force_param = "force=true" if force_scrape else ""
    scrape_cmd = f"export AWS_ACCOUNT={aws_account} && make scrape-month YEAR={year} MONTH={month} {force_param}"

    # Run the scrape command
    success, output = run_command(scrape_cmd)
    if not success:
        logger.error(f"Failed to start scrape for {year}-{month:02d}")
        return False

    # Extract the execution ARN
    execution_arn = extract_execution_arn(output)
    if not execution_arn:
        logger.error(f"Could not extract execution ARN from output: {output}")
        return False

    # Wait for the scrape job to complete if requested
    if wait_for_completion:
        if not check_execution_status(execution_arn):
            logger.error(f"Scrape job failed or timed out for {year}-{month:02d}")
            return False

    # Process data
    logger.info(f"Starting processing after scrape for {year}-{month:02d}")
    process_cmd = f"export AWS_ACCOUNT={aws_account} && make process-data"

    success, output = run_command(process_cmd)
    if not success:
        logger.error(f"Failed to start processing after scrape for {year}-{month:02d}")
        return False

    # Extract the execution ARN for the processing job
    processing_arn = extract_execution_arn(output)
    if not processing_arn:
        logger.error(f"Could not extract processing execution ARN from output: {output}")
        return False

    # Wait for the processing job to complete if requested
    if wait_for_completion:
        if not check_execution_status(processing_arn):
            logger.error(f"Processing job failed or timed out for {year}-{month:02d}")
            return False

    logger.info(f"Successfully completed scrape and process for {year}-{month:02d}")
    return True

def backfill_months(start_year, start_month, end_year, end_month, aws_account, force_scrape=False, wait_for_completion=True):
    """Backfill scrape and process for a range of months."""
    current_year = start_year
    current_month = start_month

    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        success = scrape_and_process_month(
            current_year,
            current_month,
            aws_account,
            force_scrape,
            wait_for_completion
        )

        if not success:
            logger.error(f"Failed to process {current_year}-{current_month:02d}, continuing with next month...")

        # Move to next month
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

def main():
    parser = argparse.ArgumentParser(description='Backfill NC Soccer data from 2007 to 2025')
    parser.add_argument('--start-year', type=int, default=2007, help='Start year (default: 2007)')
    parser.add_argument('--start-month', type=int, default=1, help='Start month (default: 1)')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='End year (default: current year)')
    parser.add_argument('--end-month', type=int, default=datetime.now().month, help='End month (default: current month)')
    parser.add_argument('--aws-account', default='552336166511', help='AWS account number')
    parser.add_argument('--force-scrape', action='store_true', help='Force re-scrape even if already scraped')
    parser.add_argument('--no-wait', action='store_true', help='Do not wait for job completion before starting next month')

    args = parser.parse_args()

    # Validate input
    if args.start_year < 2007:
        logger.warning("Start year is before 2007, setting to 2007")
        args.start_year = 2007

    if args.end_year > datetime.now().year:
        logger.warning(f"End year is in the future, setting to current year ({datetime.now().year})")
        args.end_year = datetime.now().year

    if args.start_year > args.end_year:
        logger.error("Start year cannot be greater than end year")
        sys.exit(1)

    if args.start_year == args.end_year and args.start_month > args.end_month:
        logger.error("Start month cannot be greater than end month within the same year")
        sys.exit(1)

    # Start the backfill process
    logger.info(f"Starting backfill from {args.start_year}-{args.start_month:02d} to {args.end_year}-{args.end_month:02d}")
    logger.info(f"Force scrape: {args.force_scrape}")
    logger.info(f"Wait for completion: {not args.no_wait}")

    backfill_months(
        args.start_year,
        args.start_month,
        args.end_year,
        args.end_month,
        args.aws_account,
        args.force_scrape,
        not args.no_wait
    )

    logger.info("Backfill process completed")

if __name__ == '__main__':
    main()
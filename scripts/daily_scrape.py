#!/usr/bin/env python3
"""
Daily scrape script for NC Soccer data.
This script can be run daily to scrape the current day and optionally a range of days.
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
        logging.FileHandler("daily_scrape.log"),
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

def run_scrape_mode(mode, year, month, day, aws_account, force_scrape=False):
    """Run a scrape job in either day or month mode."""
    mode_arg = f"--mode {mode}"
    year_arg = f"--year {year}"
    month_arg = f"--month {month}"
    day_arg = f"--day {day}" if day is not None and mode == "day" else ""
    force_arg = "--force-scrape" if force_scrape else ""

    command = (f"AWS_PROFILE=mzakany python scripts/trigger_step_function.py "
               f"--state-machine-arn arn:aws:states:us-east-2:{aws_account}:stateMachine:ncsoccer-workflow "
               f"{mode_arg} {year_arg} {month_arg} {day_arg} {force_arg}")

    success, output = run_command(command)
    if not success:
        logger.error(f"Failed to start scrape job for {year}-{month:02d}{'-'+str(day) if day else ''}")
        return None

    execution_arn = extract_execution_arn(output)
    if not execution_arn:
        logger.error(f"Could not extract execution ARN from output: {output}")
        return None

    return execution_arn

def run_process_job(aws_account):
    """Run the processing job."""
    command = f"export AWS_ACCOUNT={aws_account} && make process-data"

    success, output = run_command(command)
    if not success:
        logger.error("Failed to start processing job")
        return None

    execution_arn = extract_execution_arn(output)
    if not execution_arn:
        logger.error(f"Could not extract processing execution ARN from output: {output}")
        return None

    return execution_arn

def scrape_specific_day(year, month, day, aws_account, force_scrape=False, wait_for_completion=True):
    """Scrape a specific day's data."""
    logger.info(f"Starting scrape for {year}-{month:02d}-{day:02d}")

    execution_arn = run_scrape_mode("day", year, month, day, aws_account, force_scrape)
    if not execution_arn:
        return False

    if wait_for_completion:
        if not check_execution_status(execution_arn):
            logger.error(f"Scrape job failed or timed out for {year}-{month:02d}-{day:02d}")
            return False

    logger.info(f"Successfully scraped {year}-{month:02d}-{day:02d}")
    return True

def scrape_date_range(start_date, end_date, aws_account, force_scrape=False, wait_for_completion=True):
    """Scrape a range of dates, one day at a time."""
    current_date = start_date
    success_count = 0

    while current_date <= end_date:
        success = scrape_specific_day(
            current_date.year,
            current_date.month,
            current_date.day,
            aws_account,
            force_scrape,
            wait_for_completion
        )

        if success:
            success_count += 1

        current_date += timedelta(days=1)

    return success_count

def main():
    parser = argparse.ArgumentParser(description='Daily scrape for NC Soccer data')
    parser.add_argument('--days-back', type=int, default=0,
                      help='Number of days to go back from today (default: 0, just scrape today)')
    parser.add_argument('--date', type=str,
                      help='Specific date to scrape in YYYY-MM-DD format (overrides days-back)')
    parser.add_argument('--start-date', type=str,
                      help='Start date for a range in YYYY-MM-DD format (requires --end-date)')
    parser.add_argument('--end-date', type=str,
                      help='End date for a range in YYYY-MM-DD format (requires --start-date)')
    parser.add_argument('--aws-account', default='552336166511', help='AWS account number')
    parser.add_argument('--force-scrape', action='store_true', help='Force re-scrape even if already scraped')
    parser.add_argument('--no-wait', action='store_true', help='Do not wait for job completion before proceeding')
    parser.add_argument('--no-process', action='store_true', help='Skip the processing step')

    args = parser.parse_args()
    wait_for_completion = not args.no_wait

    # Parse date arguments
    today = datetime.today()

    if args.date:
        try:
            specific_date = datetime.strptime(args.date, '%Y-%m-%d')
            start_date = end_date = specific_date
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD format.")
            sys.exit(1)
    elif args.start_date and args.end_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

            if start_date > end_date:
                logger.error("Start date cannot be after end date")
                sys.exit(1)
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD format.")
            sys.exit(1)
    elif args.days_back > 0:
        start_date = today - timedelta(days=args.days_back)
        end_date = today
    else:
        # Default: just today
        start_date = end_date = today

    logger.info(f"Will scrape dates from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # Run scraping
    success_count = scrape_date_range(
        start_date,
        end_date,
        args.aws_account,
        args.force_scrape,
        wait_for_completion
    )

    logger.info(f"Successfully scraped {success_count} days")

    # Run processing if not disabled
    if not args.no_process:
        logger.info("Starting data processing")
        process_arn = run_process_job(args.aws_account)

        if process_arn and wait_for_completion:
            if check_execution_status(process_arn):
                logger.info("Processing completed successfully")
            else:
                logger.error("Processing failed or timed out")
                sys.exit(1)
    else:
        logger.info("Processing step skipped as requested")

    logger.info("Daily scrape process completed")

if __name__ == '__main__':
    main()
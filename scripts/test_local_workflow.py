#!/usr/bin/env python3

import argparse
import json
import logging
import subprocess
import os
from datetime import datetime
import calendar
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_input_validator(start_date, end_date, force_scrape=False,
                      architecture_version='v1', batch_size=3,
                      bucket_name='ncsh-app-data'):
    """
    Run the input validator locally
    """
    logger.info("Running input validator")

    try:
        # Import the input validator handler
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../utils/src')))
        from input_validator import handler as input_validator_handler

        # Prepare input for the validator
        input_data = {
            "start_date": start_date,
            "end_date": end_date,
            "force_scrape": force_scrape,
            "architecture_version": architecture_version,
            "batch_size": batch_size,
            "bucket_name": bucket_name
        }

        # Run the validator
        result = input_validator_handler(input_data, None)

        logger.info(f"Input validator result: {json.dumps(result, indent=2)}")
        return result
    except Exception as e:
        logger.error(f"Error running input validator: {str(e)}")
        raise

def run_batch_planner(start_date, end_date, batch_size=3):
    """
    Run the batch planner locally
    """
    logger.info("Running batch planner")

    try:
        # Import the batch planner handler
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../utils/src')))
        from batch_planner import handler as batch_planner_handler

        # Prepare input for the planner
        input_data = {
            "start_date": start_date,
            "end_date": end_date,
            "batch_size": batch_size
        }

        # Run the planner
        result = batch_planner_handler(input_data, None)

        logger.info(f"Batch planner result: {json.dumps(result, indent=2)}")
        return result
    except Exception as e:
        logger.error(f"Error running batch planner: {str(e)}")
        raise

def run_scrapy_for_batch(start_date, end_date, force_scrape=False, storage_type='file'):
    """
    Run scrapy for a batch of dates
    """
    logger.info(f"Running scrapy for batch: {start_date} to {end_date}")

    try:
        # Build the command
        cmd = [
            "cd", "scraping", "&&",
            "scrapy", "crawl", "schedule",
            "-a", f"mode=range",
            "-a", f"start_year={start_date.split('-')[0]}",
            "-a", f"start_month={start_date.split('-')[1]}",
            "-a", f"start_day={start_date.split('-')[2]}",
            "-a", f"end_year={end_date.split('-')[0]}",
            "-a", f"end_month={end_date.split('-')[1]}",
            "-a", f"end_day={end_date.split('-')[2]}",
            "-a", f"storage_type={storage_type}"
        ]

        if force_scrape:
            cmd.extend(["-a", "force_scrape=true"])

        # Convert list to string for shell execution
        cmd_str = " ".join(cmd)

        # Run the command
        logger.info(f"Executing: {cmd_str}")
        result = subprocess.run(cmd_str, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"Scrapy failed with return code {result.returncode}")
            logger.error(f"stderr: {result.stderr}")
            return {
                "success": False,
                "error": f"Scrapy failed with return code {result.returncode}"
            }

        logger.info(f"Scrapy completed successfully for batch {start_date} to {end_date}")

        # Extract dates processed from output
        dates_processed = 0
        for line in result.stdout.split('\n'):
            if "Successfully stored" in line and "games for" in line:
                dates_processed += 1

        return {
            "success": True,
            "dates_processed": dates_processed,
            "start_date": start_date,
            "end_date": end_date
        }
    except Exception as e:
        logger.error(f"Error running scrapy: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

def run_batch_verifier(batch_results):
    """
    Run the batch verifier locally
    """
    logger.info("Running batch verifier")

    try:
        # Import the batch verifier handler
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../utils/src')))
        from batch_verifier import handler as batch_verifier_handler

        # Prepare input for the verifier
        input_data = {
            "batch_results": [{"Payload": result} for result in batch_results]
        }

        # Run the verifier
        result = batch_verifier_handler(input_data, None)

        logger.info(f"Batch verifier result: {json.dumps(result, indent=2)}")
        return result
    except Exception as e:
        logger.error(f"Error running batch verifier: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Test NC Soccer Scraper Unified Workflow Locally')

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
    parser.add_argument('--storage-type', choices=['file', 's3'], default='file',
                      help='Storage type to use (default: file)')
    parser.add_argument('--architecture', choices=['v1', 'v2'], default='v1',
                      help='Data architecture version to use (default: v1)')
    parser.add_argument('--bucket', default='ncsh-app-data',
                      help='S3 bucket name for s3 storage type (default: ncsh-app-data)')

    args = parser.parse_args()

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

    try:
        # Step 1: Run input validator
        validated_input = run_input_validator(
            start_date=start_date,
            end_date=end_date,
            force_scrape=args.force_scrape,
            architecture_version=args.architecture,
            batch_size=args.batch_size,
            bucket_name=args.bucket
        )

        # If validation failed, exit
        if isinstance(validated_input, dict) and validated_input.get('statusCode', 200) != 200:
            logger.error(f"Input validation failed: {validated_input.get('error', 'Unknown error')}")
            return

        # Step 2: Run batch planner
        batches = run_batch_planner(
            start_date=validated_input["start_date"],
            end_date=validated_input["end_date"],
            batch_size=validated_input["batch_size"]
        )

        # Step 3: Run scrapy for each batch
        batch_results = []
        for batch in batches["batches"]:
            logger.info(f"Processing batch: {batch['start_date']} to {batch['end_date']}")

            result = run_scrapy_for_batch(
                start_date=batch["start_date"],
                end_date=batch["end_date"],
                force_scrape=validated_input["force_scrape"],
                storage_type=args.storage_type
            )

            batch_results.append(result)

        # Step 4: Run batch verifier
        verification = run_batch_verifier(batch_results)

        # Print results
        logger.info("=== Workflow completed ===")
        logger.info(f"Total days processed: {verification.get('total_dates_processed', 0)}")
        logger.info(f"Successful batches: {verification.get('successful_batches', 0)} of {verification.get('total_batches', 0)}")
        logger.info(f"Success: {verification.get('success', False)}")

        if not verification.get('success', False):
            logger.error("Some batches failed. Check the logs for details.")
            logger.error(f"Failed batches: {verification.get('failed_batches', [])}")

    except Exception as e:
        logger.error(f"Error running workflow: {str(e)}")
        raise

if __name__ == '__main__':
    main()
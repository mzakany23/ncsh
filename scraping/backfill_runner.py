#!/usr/bin/env python3
"""
Efficient backfill runner script for NC Soccer data.

This script runs the backfill operation which navigates through months in reverse chronological order,
utilizing the SimpleScraper implementation to efficiently process historical data.
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
import time

# Set up logging
# Use /tmp directory for log files in Lambda (the only writable directory)
log_file = "/tmp/backfill_spider.log" if os.environ.get('AWS_LAMBDA_FUNCTION_NAME') else "backfill_spider.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_backfill(start_year=2007, start_month=1, end_year=None, end_month=None,
                 storage_type='s3', bucket_name=None, html_prefix='data/html',
                 json_prefix='data/json', lookup_file='data/lookup.json',
                 lookup_type='file', region='us-east-2', table_name=None,
                 force_scrape=False, timeout=900):
    """
    Run the backfill process to scrape historical data using SimpleScraper.

    Args:
        start_year (int): The starting year for backfill (earliest)
        start_month (int): The starting month for backfill (earliest)
        end_year (int): The ending year for backfill (latest), defaults to current year
        end_month (int): The ending month for backfill (latest), defaults to current month
        storage_type (str): Where to store results - 'file' or 's3'
        bucket_name (str): S3 bucket name if storage_type='s3'
        html_prefix (str): Prefix for HTML files
        json_prefix (str): Prefix for JSON files
        lookup_file (str): Path to lookup file
        lookup_type (str): Type of lookup - 'file' or 'dynamodb'
        region (str): AWS region for S3 and DynamoDB
        force_scrape (bool): Whether to force re-scrape of days
        timeout (int): Maximum execution time in seconds

    Returns:
        dict: Results of the backfill operation
    """
    logger.info(f"Starting backfill from {start_year}-{start_month} "
                f"to {end_year or 'current year'}-{end_month or 'current month'}")

    # Import the scraper implementation
    from ncsoccer.scraper import SimpleScraper

    # Set end date to current date if not specified
    if not end_year or not end_month:
        now = datetime.now()
        end_year = end_year or now.year
        end_month = end_month or now.month

    # Configure storage settings
    storage_config = {
        'storage_type': storage_type,
        'bucket_name': bucket_name,
        'html_prefix': html_prefix,
        'json_prefix': json_prefix,
        'lookup_file': lookup_file,
        'lookup_type': lookup_type,
        'region': region,
        'table_name': table_name
    }

    # Create start time and timeout tracker
    start_time = time.time()

    def check_timeout():
        """Check if we've exceeded the timeout limit"""
        elapsed = time.time() - start_time
        if elapsed > timeout:
            logger.warning(f"Timeout reached after {elapsed:.1f} seconds")
            return True
        return False

    # Configure and run the scraper
    try:
        scraper = SimpleScraper(
            mode='backfill',
            start_year=start_year,
            start_month=start_month,
            end_year=end_year,
            end_month=end_month,
            force_scrape=force_scrape,
            storage_type=storage_type,
            bucket_name=bucket_name,
            html_prefix=html_prefix,
            json_prefix=json_prefix,
            lookup_file=lookup_file,
            lookup_type=lookup_type,
            region=region,
            table_name=table_name
        )

        # Run the scraper with timeout checking
        result = scraper.run(timeout_callback=check_timeout)

        # Log completion and return results
        elapsed = time.time() - start_time
        logger.info(f"Backfill completed in {elapsed:.1f} seconds")
        logger.info(f"Processed months from {start_year}-{start_month} to {end_year}-{end_month}")

        return {
            'success': True,
            'processed_months': result.get('processed_months', 0),
            'processed_days': result.get('processed_days', 0),
            'elapsed_seconds': elapsed,
            'start_year': start_year,
            'start_month': start_month,
            'end_year': end_year,
            'end_month': end_month
        }

    except Exception as e:
        logger.error(f"Error in backfill: {str(e)}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'start_year': start_year,
            'start_month': start_month,
            'end_year': end_year,
            'end_month': end_month
        }

def lambda_handler(event, context):
    """
    AWS Lambda handler for backfill operations

    Args:
        event (dict): Lambda event containing backfill parameters
        context: Lambda context

    Returns:
        dict: Results of the backfill operation
    """
    logger.info(f"Received event: {json.dumps(event)}")

    # Extract parameters from event
    params = {}
    params['start_year'] = int(event.get('start_year', 2007))
    params['start_month'] = int(event.get('start_month', 1))
    params['end_year'] = int(event.get('end_year')) if event.get('end_year') else None
    params['end_month'] = int(event.get('end_month')) if event.get('end_month') else None
    params['storage_type'] = event.get('storage_type', 's3')
    params['bucket_name'] = event.get('bucket_name')
    params['html_prefix'] = event.get('html_prefix', 'data/html')
    params['json_prefix'] = event.get('json_prefix', 'data/json')
    params['lookup_file'] = event.get('lookup_file', 'data/lookup.json')
    params['lookup_type'] = event.get('lookup_type', 'file')
    params['region'] = event.get('region', 'us-east-2')
    params['table_name'] = event.get('table_name')
    params['force_scrape'] = event.get('force_scrape', False)

    # Calculate timeout, leaving buffer for lambda shutdown
    max_duration = context.get_remaining_time_in_millis() / 1000 if context else 900
    params['timeout'] = min(max_duration - 30, 870)  # 30s buffer, max 870s (14.5min)

    # Run backfill
    return run_backfill(**params)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run backfill for NC Soccer data')
    parser.add_argument('--start-year', type=int, default=2007, help='Start year (earliest)')
    parser.add_argument('--start-month', type=int, default=1, help='Start month (earliest)')
    parser.add_argument('--end-year', type=int, help='End year (latest, defaults to current year)')
    parser.add_argument('--end-month', type=int, help='End month (latest, defaults to current month)')
    parser.add_argument('--storage-type', default='file', choices=['file', 's3'], help='Storage type')
    parser.add_argument('--bucket-name', help='S3 bucket name')
    parser.add_argument('--html-prefix', default='data/html', help='Prefix for HTML files')
    parser.add_argument('--json-prefix', default='data/json', help='Prefix for JSON files')
    parser.add_argument('--lookup-file', default='data/lookup.json', help='Path to lookup file')
    parser.add_argument('--lookup-type', default='file', choices=['file', 'dynamodb'], help='Lookup type')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--table-name', help='DynamoDB table name')
    parser.add_argument('--force-scrape', action='store_true', help='Force re-scrape')
    parser.add_argument('--timeout', type=int, default=900, help='Maximum execution time in seconds')

    args = parser.parse_args()
    result = run_backfill(**vars(args))
    print(json.dumps(result, indent=2))
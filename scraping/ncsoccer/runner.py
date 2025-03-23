#!/usr/bin/env python3
"""Runner script for soccer schedule scraper with lookup functionality."""

import os
import json
import time
import argparse
import logging
from datetime import datetime, timedelta
from calendar import monthrange

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def load_lookup_data(lookup_file='data/lookup.json', storage_type='file', bucket_name=None, region='us-east-2'):
    """Load the lookup data from JSON file or S3.

    Args:
        lookup_file (str): Path to the lookup JSON file.
        storage_type (str): 'file' or 's3'
        bucket_name (str): S3 bucket name if storage_type is 's3'
        region (str): AWS region for S3

    Returns:
        dict: Dictionary containing scraped dates data.
    """
    # Detect Lambda environment - if we're in Lambda, ensure we use S3
    in_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
    if in_lambda and storage_type == 'file':
        logger.warning("Running in Lambda environment - forcing S3 storage type")
        storage_type = 's3'
        # Get bucket name from environment if not provided and we're in Lambda
        if not bucket_name:
            bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')

    if storage_type == 's3':
        # Import here to avoid circular imports
        from ncsoccer.pipeline.config import get_storage_interface
        # Get S3 storage interface
        storage = get_storage_interface('s3', bucket_name, region)
        try:
            # Check if lookup file exists in S3
            if not storage.exists(lookup_file):
                # Create initial data
                initial_data = {'scraped_dates': {}}
                storage.write(lookup_file, json.dumps(initial_data))
                return {}

            # Read lookup data from S3
            data = json.loads(storage.read(lookup_file))
            return data.get('scraped_dates', {})
        except Exception as e:
            logger.error(f"Error loading lookup file from S3: {e}")
            return {}
    else:
        # Local file system
        try:
            if not os.path.exists(lookup_file):
                os.makedirs(os.path.dirname(lookup_file), exist_ok=True)
                with open(lookup_file, 'w') as f:
                    json.dump({'scraped_dates': {}}, f)
                return {}

            with open(lookup_file, 'r') as f:
                data = json.load(f)
                return data.get('scraped_dates', {})
        except Exception as e:
            logger.error(f"Error loading lookup file from local filesystem: {e}")
            return {}


def is_date_scraped(date_str, lookup_data):
    """Check if a date has already been scraped successfully.

    Args:
        date_str (str): Date string in YYYY-MM-DD format.
        lookup_data (dict): Dictionary containing scraped dates data.

    Returns:
        bool: True if date was successfully scraped, False otherwise.
    """
    return date_str in lookup_data and lookup_data[date_str]['success']


def wait_for_file(storage, path, max_wait):
    """Wait for a file to be created in storage.

    Args:
        storage: Storage interface instance
        path (str): Path to file
        max_wait (int): Maximum seconds to wait

    Returns:
        bool: True if file exists, False otherwise
    """
    logger.info(f"Waiting for file to be created: {path}")
    start_time = time.time()

    while time.time() - start_time < max_wait:
        if storage.exists(path):
            logger.info(f"File exists: {path}")
            return True

        # Wait a bit before checking again
        time.sleep(5)

    logger.error(f"Timed out waiting for file: {path}")
    return False


def run_scraper(year=None, month=None, day=None, storage_type='s3', bucket_name=None,
               html_prefix='data/html', json_prefix='data/json', lookup_type='file', lookup_file='data/lookup.json',
               table_name=None, region='us-east-2', force_scrape=False, skip_wait=False, use_test_data=False,
               architecture_version='v1', max_wait=300):
    """Run the ncsoccer scraper

    Args:
        year (int): Year to scrape
        month (int): Month to scrape
        day (int): Day to scrape
        storage_type (str): Storage type ('file' or 's3')
        bucket_name (str): S3 bucket name if using S3 storage
        html_prefix (str): Prefix for HTML files
        json_prefix (str): Prefix for JSON files
        lookup_type (str): Type of lookup ('file' only supported)
        lookup_file (str): Path to lookup file
        table_name (str): DynamoDB table name (deprecated)
        region (str): AWS region
        force_scrape (bool): Whether to force scrape even if date exists
        skip_wait (bool): Whether to skip waiting for file creation
        use_test_data (bool): Whether to use test data paths
        architecture_version (str): Data architecture version ('v1' or 'v2')
        max_wait (int): Maximum seconds to wait for file creation

    Returns:
        dict: Result dictionary with success status and other information
    """
    try:
        # Detect Lambda environment - if we're in Lambda, ensure we use S3
        in_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
        if in_lambda:
            if storage_type != 's3' or lookup_type != 's3':
                logger.warning("Running in Lambda environment - forcing S3 storage and lookup types")
                storage_type = 's3'
                lookup_type = 's3'

            # Get bucket name from environment if not provided and we're in Lambda
            if not bucket_name:
                bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')

            # Handle path adjustments based on architecture version
            # Make this more robust to handle different architecture version formats
            arch_version_str = architecture_version.lower() if isinstance(architecture_version, str) else getattr(architecture_version, 'value', 'v2').lower()
            
            if arch_version_str == 'v1':
                # For v1 architecture, ensure directories start with /tmp in Lambda to avoid read-only filesystem errors
                if not html_prefix.startswith('/tmp/') and not html_prefix.startswith('s3://'):
                    html_prefix = f'/tmp/{html_prefix}'
                    logger.info(f"Adjusted html_prefix for Lambda (v1): {html_prefix}")

                if not json_prefix.startswith('/tmp/') and not json_prefix.startswith('s3://'):
                    json_prefix = f'/tmp/{json_prefix}'
                    logger.info(f"Adjusted json_prefix for Lambda (v1): {json_prefix}")

                if not lookup_file.startswith('/tmp/') and not lookup_file.startswith('s3://'):
                    lookup_file = f'/tmp/{lookup_file}'
                    logger.info(f"Adjusted lookup_file for Lambda (v1): {lookup_file}")
            elif arch_version_str == 'v2':
                # For v2 architecture, ensure we're NOT using /tmp paths
                if html_prefix and html_prefix.startswith('/tmp/'):
                    html_prefix = html_prefix.replace('/tmp/', '')
                    logger.info(f"Removed /tmp prefix from html_prefix for v2 architecture: {html_prefix}")
                
                if json_prefix and json_prefix.startswith('/tmp/'):
                    json_prefix = json_prefix.replace('/tmp/', '')
                    logger.info(f"Removed /tmp prefix from json_prefix for v2 architecture: {json_prefix}")
                
                if lookup_file and lookup_file.startswith('/tmp/'):
                    lookup_file = lookup_file.replace('/tmp/', '')
                    logger.info(f"Removed /tmp prefix from lookup_file for v2 architecture: {lookup_file}")

        # Get current date for defaults
        now = datetime.now()
        year = year or now.year
        month = month or now.month
        day = day or now.day

        date_str = f"{year}-{month:02d}-{day:02d}"
        logger.info(f"Running scraper for {date_str}")

        # Check if date has already been scraped
        if not force_scrape and lookup_type == 'file':
            lookup_data = load_lookup_data(lookup_file, storage_type, bucket_name, region)
            if is_date_scraped(date_str, lookup_data):
                logger.info(f"Already scraped {date_str}, skipping")
                return {"success": True, "skipped": True, "date": date_str}

        # Import the SimpleScraper to run
        from ncsoccer.scraper import SimpleScraper

        # Create and run the scraper
        scraper = SimpleScraper(
            mode='day',
            start_year=year,
            start_month=month,
            start_day=day,
            storage_type=storage_type,
            bucket_name=bucket_name,
            html_prefix=html_prefix,
            json_prefix=json_prefix,
            lookup_file=lookup_file,
            lookup_type=lookup_type,
            region=region,
            force_scrape=force_scrape
        )

        # Run the scraper
        result = scraper.run()

        if not result.get('success', False):
            logger.error(f"Scraper failed: {result.get('error', 'Unknown error')}")
            return result

        # If we're using architecture v2, we're done
        if architecture_version == 'v2':
            logger.info(f"Scraper completed successfully for {date_str}")
            return result

        # For v1 architecture, if storage is s3 and we need to wait for file creation
        if storage_type == 's3' and not skip_wait:
            from ncsoccer.pipeline.config import get_storage_interface
            storage = get_storage_interface(storage_type, bucket_name, region=region)

            # Check for JSON files
            json_path = f"{json_prefix}/{date_str}.json"
            json_meta_path = f"{json_prefix}/{date_str}_meta.json"

            # Wait for JSON file
            if not wait_for_file(storage, json_path, max_wait):
                logger.error(f"Timed out waiting for JSON file: {json_path}")
                return {"success": False, "error": f"Timed out waiting for JSON file: {json_path}"}

            # Wait for JSON meta file
            if not wait_for_file(storage, json_meta_path, max_wait):
                logger.error(f"Timed out waiting for JSON meta file: {json_meta_path}")
                return {"success": False, "error": f"Timed out waiting for JSON meta file: {json_meta_path}"}

        # Update lookup data
        if lookup_type == 'file':
            update_lookup_data(
                None,  # We'll load lookup data inside the function
                date_str,
                success=True,
                games_count=result.get('games_count', 0),
                lookup_file=lookup_file,
                storage_type=storage_type,
                bucket_name=bucket_name,
                region=region
            )

        logger.info(f"Scraper completed successfully for {date_str}")
        return {"success": True, "date_str": date_str, "games_count": result.get('games_count', 0)}

    except Exception as e:
        logger.error(f"Error running scraper: {str(e)}", exc_info=True)
        return {"success": False, "error": str(e), "date_str": date_str if 'date_str' in locals() else None}


def run_month(year=None, month=None, storage_type='s3', bucket_name=None,
              html_prefix='data/html', json_prefix='data/json', lookup_file='data/lookup.json',
              lookup_type='file', region='us-east-2', target_days=None, table_name=None,
              force_scrape=False, use_test_data=False, max_retries=3, architecture_version='v1',
              max_wait=300):
    """Run the scraper for an entire month

    Args:
        year (int): Year to scrape
        month (int): Month to scrape
        storage_type (str): Storage type ('file' or 's3')
        bucket_name (str): S3 bucket name if using S3 storage
        html_prefix (str): Prefix for HTML files
        json_prefix (str): Prefix for JSON files
        lookup_file (str): Path to lookup file
        lookup_type (str): Type of lookup ('file' or 'dynamodb')
        region (str): AWS region
        target_days (list): List of days to scrape in the month (default: all days)
        table_name (str): DynamoDB table name
        force_scrape (bool): Whether to force scrape even if date exists
        use_test_data (bool): Whether to use test data paths
        max_retries (int): Maximum number of retries for a failed day
        architecture_version (str): Data architecture version ('v1' or 'v2')
        max_wait (int): Maximum seconds to wait for file creation

    Returns:
        dict: Result dictionary with success status and other information
    """
    try:
        # Get current date for defaults
        now = datetime.now()
        year = year or now.year
        month = month or now.month

        logger.info(f"Running scraper for month: {year}-{month:02d}")

        # Get the number of days in the month
        _, num_days = monthrange(year, month)

        # If target_days is None, scrape all days
        if target_days is None:
            target_days = list(range(1, num_days + 1))
        else:
            # Validate target days
            target_days = [d for d in target_days if 1 <= d <= num_days]
            target_days.sort()  # Sort for consistent order

        if not target_days:
            logger.warning(f"No valid days to scrape for {year}-{month:02d}")
            return {"success": True, "skipped": True, "month": f"{year}-{month:02d}"}

        logger.info(f"Will scrape {len(target_days)} days in {year}-{month:02d}: {target_days}")

        # Import the SimpleScraper to run
        from ncsoccer.scraper import SimpleScraper

        # Create and run the scraper
        scraper = SimpleScraper(
            mode='month',
            start_year=year,
            start_month=month,
            storage_type=storage_type,
            bucket_name=bucket_name,
            html_prefix=html_prefix,
            json_prefix=json_prefix,
            lookup_file=lookup_file,
            lookup_type=lookup_type,
            region=region,
            force_scrape=force_scrape,
            target_days=target_days
        )

        # Run the scraper
        result = scraper.run()

        if not result.get('success', False):
            logger.error(f"Scraper failed for month {year}-{month:02d}: {result.get('error', 'Unknown error')}")
            return result

        # Track results
        processed_days = result.get('days_processed', 0)
        total_games = result.get('games_count', 0)

        logger.info(f"Scraper completed for month {year}-{month:02d}")
        logger.info(f"Processed {processed_days} days and found {total_games} games")

        return {
            "success": True,
            "month": f"{year}-{month:02d}",
            "days_processed": processed_days,
            "games_count": total_games
        }

    except Exception as e:
        logger.error(f"Error running month scraper: {str(e)}", exc_info=True)
        return {"success": False, "error": str(e), "month": f"{year}-{month:02d}" if 'year' in locals() and 'month' in locals() else None}


def run_date_range(start_date, end_date, storage_type='s3', bucket_name=None,
                  html_prefix='data/html', json_prefix='data/json', lookup_file='data/lookup.json',
                  lookup_type='file', region='us-east-2', force_scrape=False, use_test_data=False,
                  architecture_version='v1', max_wait=300):
    """Run the scraper for a date range

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format
        end_date (str): End date in 'YYYY-MM-DD' format
        storage_type (str): Storage type ('file' or 's3')
        bucket_name (str): S3 bucket name if using S3 storage
        html_prefix (str): Prefix for HTML files
        json_prefix (str): Prefix for JSON files
        lookup_file (str): Path to lookup file
        lookup_type (str): Type of lookup ('file' or 'dynamodb')
        region (str): AWS region
        force_scrape (bool): Whether to force scrape even if date exists
        use_test_data (bool): Whether to use test data paths
        architecture_version (str): Data architecture version ('v1' or 'v2')
        max_wait (int): Maximum seconds to wait for file creation

    Returns:
        dict: Result dictionary with success status and other information
    """
    try:
        # Parse start and end dates
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')

        if end_dt < start_dt:
            logger.error(f"End date {end_date} is before start date {start_date}")
            return {"success": False, "error": f"End date {end_date} is before start date {start_date}"}

        logger.info(f"Running scraper for date range: {start_date} to {end_date}")

        # Import the SimpleScraper to run
        from ncsoccer.scraper import SimpleScraper

        # Create and run the scraper
        scraper = SimpleScraper(
            mode='range',
            start_year=start_dt.year,
            start_month=start_dt.month,
            start_day=start_dt.day,
            end_year=end_dt.year,
            end_month=end_dt.month,
            end_day=end_dt.day,
            storage_type=storage_type,
            bucket_name=bucket_name,
            html_prefix=html_prefix,
            json_prefix=json_prefix,
            lookup_file=lookup_file,
            lookup_type=lookup_type,
            region=region,
            force_scrape=force_scrape
        )

        # Run the scraper
        result = scraper.run()

        if not result.get('success', False):
            logger.error(f"Scraper failed for date range {start_date} to {end_date}: {result.get('error', 'Unknown error')}")
            return result

        # Track results
        processed_days = result.get('days_processed', 0)
        total_games = result.get('games_count', 0)

        logger.info(f"Scraper completed for date range {start_date} to {end_date}")
        logger.info(f"Processed {processed_days} days and found {total_games} games")

        return {
            "success": True,
            "start_date": start_date,
            "end_date": end_date,
            "days_processed": processed_days,
            "games_count": total_games
        }

    except Exception as e:
        logger.error(f"Error running range scraper: {str(e)}", exc_info=True)
        return {"success": False, "error": str(e), "start_date": start_date, "end_date": end_date}


def update_lookup_data(lookup_data, date_str, success=True, games_count=0, lookup_file='data/lookup.json',
                   storage_type='file', bucket_name=None, region='us-east-2'):
    """Update the lookup data with the result of a scrape.

    Args:
        lookup_data (dict): Dictionary containing scraped dates data. If None, will be loaded.
        date_str (str): Date string in YYYY-MM-DD format.
        success (bool): Whether the scrape was successful.
        games_count (int): Number of games scraped.
        lookup_file (str): Path to the lookup JSON file.
        storage_type (str): 'file' or 's3'
        bucket_name (str): S3 bucket name if storage_type is 's3'
        region (str): AWS region for S3

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        # Load lookup data if not provided
        if lookup_data is None:
            lookup_data = load_lookup_data(lookup_file, storage_type, bucket_name, region)

        # Update lookup data
        lookup_data[date_str] = {
            'success': success,
            'games_count': games_count,
            'timestamp': datetime.now().isoformat()
        }

        if storage_type == 's3':
            # Import here to avoid circular imports
            from ncsoccer.pipeline.config import get_storage_interface
            # Get S3 storage interface
            storage = get_storage_interface('s3', bucket_name, region)
            try:
                # Read full lookup file
                if storage.exists(lookup_file):
                    data = json.loads(storage.read(lookup_file))
                else:
                    data = {'scraped_dates': {}}

                # Update scraped_dates
                data['scraped_dates'] = lookup_data

                # Write back to S3
                storage.write(lookup_file, json.dumps(data))
                return True
            except Exception as e:
                logger.error(f"Error updating lookup file in S3: {e}")
                return False
        else:
            # Local file system
            try:
                if not os.path.exists(os.path.dirname(lookup_file)):
                    os.makedirs(os.path.dirname(lookup_file), exist_ok=True)

                if os.path.exists(lookup_file):
                    with open(lookup_file, 'r') as f:
                        data = json.load(f)
                else:
                    data = {'scraped_dates': {}}

                # Update scraped_dates
                data['scraped_dates'] = lookup_data

                # Write back to file
                with open(lookup_file, 'w') as f:
                    json.dump(data, f)
                return True
            except Exception as e:
                logger.error(f"Error updating lookup file in local filesystem: {e}")
                return False
    except Exception as e:
        logger.error(f"Error in update_lookup_data: {e}")
        return False


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='NC Soccer Schedule Scraper')
    parser.add_argument('--year', type=int, required=True, help='Year to scrape')
    parser.add_argument('--month', type=int, required=True, help='Month to scrape')
    parser.add_argument('--day', type=int, help='Day to scrape (optional)')
    parser.add_argument('--mode', choices=['day', 'month'], default='day', help='Scraping mode')
    parser.add_argument('--force-scrape', action='store_true', help='Force scrape even if already done')
    parser.add_argument('--storage-type', choices=['file', 's3'], default='s3', help='Storage type')
    parser.add_argument('--bucket-name', default='ncsh-app-data', help='S3 bucket name')
    parser.add_argument('--html-prefix', default='data/html', help='HTML prefix')
    parser.add_argument('--json-prefix', default='data/json', help='JSON prefix')
    parser.add_argument('--lookup-type', choices=['file', 'dynamodb'], default='file', help='Lookup type')
    parser.add_argument('--lookup-file', default='data/lookup.json', help='Path to lookup file')
    parser.add_argument('--table-name', default='ncsh-scraped-dates', help='DynamoDB table name')
    parser.add_argument('--architecture-version', default='v1', help='Architecture version')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--max-wait', type=int, default=300, help='Maximum seconds to wait for file creation')
    parser.add_argument('--use-test-data', action='store_true', help='Use test data paths')

    args = parser.parse_args()

    # Common parameters
    common_params = {
        'storage_type': args.storage_type,
        'bucket_name': args.bucket_name,
        'html_prefix': args.html_prefix,
        'json_prefix': args.json_prefix,
        'lookup_type': args.lookup_type,
        'lookup_file': args.lookup_file,
        'region': args.region,
        'table_name': args.table_name,
        'force_scrape': args.force_scrape,
        'architecture_version': args.architecture_version,
        'max_wait': args.max_wait,
        'use_test_data': args.use_test_data
    }

    if args.mode == 'day' and args.day:
        result = run_scraper(
            year=args.year,
            month=args.month,
            day=args.day,
            **common_params
        )
    else:
        result = run_month(
            year=args.year,
            month=args.month,
            **common_params
        )

    print(f"Scraper {'succeeded' if result['success'] else 'failed'}")
    import sys
    sys.exit(0 if result['success'] else 1)
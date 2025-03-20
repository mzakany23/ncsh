#!/usr/bin/env python3
"""Runner script for soccer schedule scraper with lookup functionality."""

import os
import json
import time
import argparse
import logging
from datetime import datetime, timedelta
from calendar import monthrange

# Try to install asyncio reactor, but don't fail if we can't
try:
    import asyncio
    import twisted.internet.asyncio
    from twisted.internet import reactor
    twisted.internet.asyncio.install()
except (ImportError, Exception) as e:
    print(f"Warning: Could not install asyncio reactor: {e}")

# Global flag to track if reactor has been started
_reactor_started = False

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_lookup_data(lookup_file='data/lookup.json'):
    """Load the lookup data from JSON file.

    Args:
        lookup_file (str): Path to the lookup JSON file.

    Returns:
        dict: Dictionary containing scraped dates data.
    """
    if not os.path.exists(lookup_file):
        os.makedirs(os.path.dirname(lookup_file), exist_ok=True)
        with open(lookup_file, 'w') as f:
            json.dump({'scraped_dates': {}}, f)
        return {}

    try:
        with open(lookup_file, 'r') as f:
            data = json.load(f)
            return data.get('scraped_dates', {})
    except Exception as e:
        logger.error(f"Error loading lookup file: {e}")
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


def run_scraper(year=None, month=None, day=None, storage_type='s3', bucket_name=None,
             html_prefix='data/html', json_prefix='data/json', lookup_file='data/lookup.json',
             lookup_type='file', region='us-east-2', table_name=None, force_scrape=False,
             use_test_data=False, architecture_version='v1'):
    """Run the scraper for a specific day

    Args:
        year (int): Year to scrape
        month (int): Month to scrape
        day (int): Day to scrape
        storage_type (str): Storage type ('file' or 's3')
        bucket_name (str): S3 bucket name if using S3 storage
        html_prefix (str): Prefix for HTML files
        json_prefix (str): Prefix for JSON files
        lookup_file (str): Path to lookup file
        lookup_type (str): Type of lookup ('file' only supported)
        region (str): AWS region
        table_name (str): DynamoDB table name (deprecated)
        force_scrape (bool): Whether to force scrape even if date exists
        use_test_data (bool): Whether to use test data paths
        architecture_version (str): Data architecture version ('v1' or 'v2')

    Returns:
        bool: Success status
    """
    try:
        logger.info(f"Starting run_scraper with params: year={year}, month={month}, day={day}, "
                   f"storage_type={storage_type}, bucket_name={bucket_name}, html_prefix={html_prefix}, "
                   f"json_prefix={json_prefix}, lookup_type={lookup_type}, force_scrape={force_scrape}, "
                   f"architecture_version={architecture_version}")

        # Just call run_month with a single day
        return run_month(year, month, storage_type, bucket_name, html_prefix, json_prefix,
                        lookup_file, lookup_type, region, target_days=[day], table_name=table_name,
                        force_scrape=force_scrape, use_test_data=use_test_data,
                        architecture_version=architecture_version)
    except Exception as e:
        logger.error(f"Error in run_scraper: {str(e)}", exc_info=True)
        raise RuntimeError(f"Error in run_scraper: {str(e)}")


def run_month(year=None, month=None, storage_type='s3', bucket_name=None,
              html_prefix='data/html', json_prefix='data/json', lookup_file='data/lookup.json',
              lookup_type='file', region='us-east-2', target_days=None, table_name=None,
              force_scrape=False, use_test_data=False, max_retries=3, architecture_version='v1'):
    """Run the scraper for specific days in a month

    Args:
        year (int): Year to scrape
        month (int): Month to scrape
        storage_type (str): Storage type ('file' or 's3')
        bucket_name (str): S3 bucket name if using S3 storage
        html_prefix (str): Prefix for HTML files
        json_prefix (str): Prefix for JSON files
        lookup_file (str): Path to lookup file
        lookup_type (str): Type of lookup ('file' only supported)
        region (str): AWS region
        target_days (list): List of days to scrape. If None, scrape all days in month
        table_name (str): DynamoDB table name (deprecated)
        force_scrape (bool): Whether to force scrape even if date exists
        use_test_data (bool): Whether to use test data paths
        max_retries (int): Maximum number of retries for failed scrapes
        architecture_version (str): Data architecture version ('v1' or 'v2')

    Returns:
        bool: Success status
    """
    retry_count = 0
    last_error = None

    # Validate architecture_version
    if architecture_version not in ('v1', 'v2'):
        logger.warning(f"Invalid architecture_version: {architecture_version}. Using v1 as default.")
        architecture_version = 'v1'

    logger.info(f"Running with architecture version: {architecture_version}")

    # Get current date for defaults
    now = datetime.now()
    year = year or now.year
    month = month or now.month

    # Fill in all days in the month if not specified
    if target_days is None:
        last_day = monthrange(year, month)[1]
        target_days = list(range(1, last_day + 1))

    # Sort target days to ensure consistent order
    target_days = sorted(target_days)

    logger.info(f"Target days: {target_days}")

    while retry_count < max_retries:
        # Overall success indicator
        success = True
        errors = []

        try:
            # Load lookup data
            lookup_data = {}
            if not force_scrape and lookup_type == 'file':
                lookup_data = load_lookup_data(lookup_file)

            # Filter target days if we should skip existing
            if not force_scrape and lookup_data:
                filtered_days = []
                for day in target_days:
                    date_str = f"{year}-{month:02d}-{day:02d}"
                    if not is_date_scraped(date_str, lookup_data):
                        filtered_days.append(day)
                    else:
                        logger.info(f"Skipping {date_str} (already scraped)")

                # If no days to scrape after filtering, return success
                if not filtered_days:
                    logger.info(f"No days to scrape for {year}-{month:02d} (all already scraped)")
                    return True

                target_days = filtered_days

            # If using v2 architecture, initialize the checkpoint interface
            checkpoint = None
            if architecture_version == 'v2':
                # Import here to avoid circular imports
                from ncsoccer.pipeline.config import DataPathManager, get_storage_interface
                from ncsoccer.pipeline.checkpoint import get_checkpoint_manager

                # Create path manager for checkpoint path
                path_manager = DataPathManager(
                    architecture_version=architecture_version,
                    base_prefix='test_data' if use_test_data else ''
                )

                # Create storage interface
                storage = get_storage_interface(storage_type, bucket_name, region=region)

                # Create checkpoint manager
                checkpoint_path = path_manager.get_checkpoint_path()
                checkpoint = get_checkpoint_manager(checkpoint_path, storage_interface=storage)
                logger.info(f"Checkpoint manager initialized for {checkpoint_path}")

                # Filter target days based on checkpoint data
                if not force_scrape and checkpoint:
                    filtered_days = []
                    for day in target_days:
                        date_str = f"{year}-{month:02d}-{day:02d}"
                        if not checkpoint.is_date_scraped(date_str):
                            filtered_days.append(day)
                        else:
                            logger.info(f"Skipping {date_str} (already scraped according to checkpoint)")

                    # If no days to scrape after filtering, return success
                    if not filtered_days:
                        logger.info(f"No days to scrape for {year}-{month:02d} (all already scraped)")
                        return True

                    target_days = filtered_days

            # Configure logging for scrapy
            configure_logging()

            # Configure scrapy settings
            settings = get_project_settings()
            settings.update({
                'LOG_LEVEL': 'INFO',
                'COOKIES_DEBUG': True,
                'DOWNLOAD_DELAY': 1,
                'CONCURRENT_REQUESTS': 1,
                'TELNETCONSOLE_ENABLED': False,  # Disable telnet console for Lambda
                'RETRY_ENABLED': True,
                'RETRY_TIMES': 3,  # Number of times to retry failed requests
                'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],  # HTTP codes to retry on
                'SPIDER_MODULES': ['ncsoccer.spiders'],  # Explicitly set spider modules
                'NEWSPIDER_MODULE': 'ncsoccer.spiders',  # Set new spider module
                'BOT_NAME': 'ncsoccer'  # Set bot name
            })
            logger.info(f"Using Scrapy settings: {settings.copy_to_dict()}")

            try:
                process = CrawlerProcess(settings)
                logger.info("Created CrawlerProcess successfully")
            except Exception as e:
                logger.error(f"Failed to create CrawlerProcess: {str(e)}", exc_info=True)
                raise

            # Create a deferred to track spider completion
            from twisted.internet import defer
            deferreds = []

            # Schedule a spider for each target day
            try:
                for day in target_days:
                    logger.info(f"Scheduling spider for {year}-{month:02d}-{day:02d}")

                    # Create spider for the day, using date_str to uniquely identify spiders
                    date_str = f"{year}-{month:02d}-{day:02d}"

                    # Configure spider
                    spider = process.create_crawler('schedule')
                    d = process.crawl(
                        spider,
                        mode='day',
                        year=year,
                        month=month,
                        day=day,
                        storage_type=storage_type,
                        bucket_name=bucket_name,
                        html_prefix=html_prefix,
                        json_prefix=json_prefix,
                        lookup_file=lookup_file,
                        lookup_type=lookup_type,
                        region=region,
                        table_name=table_name,
                        force_scrape=force_scrape,
                        use_test_data=use_test_data,
                        architecture_version=architecture_version
                    )
                    deferreds.append(d)
                    logger.info(f"Spider scheduled for {date_str}")

                logger.info(f"Scheduled {len(deferreds)} spiders")

                # Start the crawl process
                logger.info("Starting crawler process")
                try:
                    process.start()
                    logger.info("Process completed")
                except Exception as e:
                    logger.error(f"Error in process.start(): {str(e)}", exc_info=True)
                    success = False
                    errors.append(str(e))
            except Exception as e:
                logger.error(f"Error scheduling spiders: {str(e)}", exc_info=True)
                success = False
                errors.append(str(e))

            if not success:
                logger.error(f"Crawl process failed with errors: {errors}")
                retry_count += 1
                last_error = errors
                continue

            # Verify files were created
            logger.info("Verifying files were created")
            max_wait = 120  # Maximum wait time in seconds
            start_time = time.time()

            # Verify files were created for all target days
            for day in target_days:
                date_str = f"{year}-{month:02d}-{day:02d}"

                # Define expected files based on storage type and test mode
                prefix = 'test_data' if use_test_data else 'data'
                logger.info(f"Verifying files with prefix: {prefix}")

                # Create path manager to check files
                from ncsoccer.pipeline.config import DataPathManager
                path_manager = DataPathManager(
                    architecture_version=architecture_version,
                    base_prefix='test_data' if use_test_data else ''
                )

                date_obj = datetime(year, month, day)

                # Get expected file paths using the path manager
                expected_files = [
                    path_manager.get_html_path(date_obj),
                    path_manager.get_json_meta_path(date_obj),
                    path_manager.get_games_path(date_obj)
                ]

                logger.info(f"Expected files to verify: {expected_files}")

                # Get the storage interface based on configuration
                from ncsoccer.pipeline.config import get_storage_interface
                storage = get_storage_interface(storage_type, bucket_name, region)
                logger.info(f"Using storage interface: {storage.__class__.__name__}")

                # Verify files using the storage interface with timeout
                for file_path in expected_files:
                    logger.info(f"Checking for file: {file_path}")
                    while True:
                        if time.time() - start_time > max_wait:
                            raise TimeoutError(f"Timeout waiting for file {file_path}")

                        if storage.exists(file_path):
                            logger.info(f"Found file: {file_path}")
                            break

                        time.sleep(5)  # Wait 5 seconds before checking again

                logger.info(f"Successfully verified files for {date_str}")

            # If we get here, everything worked
            return True

        except Exception as e:
            logger.error(f"Error in run_month (attempt {retry_count + 1}): {str(e)}", exc_info=True)
            retry_count += 1
            last_error = str(e)
            time.sleep(2)  # Brief delay before retry

    # If we get here, all retries failed
    logger.error(f"All {max_retries} attempts failed. Last error: {last_error}")
    raise RuntimeError(f"Failed to run month after {max_retries} attempts. Last error: {last_error}")


def run_date_range(start_date, end_date, lookup_file='data/lookup.json',
                  skip_existing=True):
    """Run scraper for a range of dates.

    Args:
        start_date (datetime): Start date to scrape from.
        end_date (datetime): End date to scrape to.
        lookup_file (str): Path to the lookup JSON file.
        skip_existing (bool): Whether to skip already scraped dates.

    Returns:
        bool: True if all dates were scraped successfully, False otherwise.
    """
    current = start_date
    failed_dates = []

    while current <= end_date:
        if not run_scraper(current.year, current.month, current.day):
            failed_dates.append(current.strftime('%Y-%m-%d'))
        current = current + timedelta(days=1)

    if failed_dates:
        logger.error(f"Failed to scrape dates: {failed_dates}")
        return False
    return True


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='NC Soccer Schedule Scraper')
    parser.add_argument('--year', type=int, required=True, help='Year to scrape')
    parser.add_argument('--month', type=int, required=True, help='Month to scrape')
    parser.add_argument('--day', type=int, help='Day to scrape (optional)')
    parser.add_argument('--mode', choices=['day', 'month'], default='day', help='Scraping mode')
    parser.add_argument('--force-scrape', action='store_true', help='Force scrape even if already done')
    parser.add_argument('--storage-type', choices=['file', 's3'], default='file', help='Storage type')
    parser.add_argument('--bucket-name', default='ncsh-app-data', help='S3 bucket name')
    parser.add_argument('--html-prefix', default='data/html', help='HTML prefix')
    parser.add_argument('--json-prefix', default='data/json', help='JSON prefix')
    parser.add_argument('--lookup-type', choices=['file', 'dynamodb'], default='file', help='Lookup type')
    parser.add_argument('--table-name', default='ncsh-scraped-dates', help='DynamoDB table name')
    parser.add_argument('--architecture-version', default='v1', help='Architecture version')

    args = parser.parse_args()

    # Common parameters
    common_params = {
        'storage_type': args.storage_type,
        'bucket_name': args.bucket_name,
        'html_prefix': args.html_prefix,
        'json_prefix': args.json_prefix,
        'lookup_type': args.lookup_type,
        'table_name': args.table_name,
        'force_scrape': args.force_scrape,
        'architecture_version': args.architecture_version
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

    print(json.dumps({"result": result}))
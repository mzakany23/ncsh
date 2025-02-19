#!/usr/bin/env python3
"""Runner script for soccer schedule scraper with lookup functionality."""

import os
import json
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
             use_test_data=False):
    """Run the scraper for a specific day"""
    try:
        logger.info(f"Starting run_scraper with params: year={year}, month={month}, day={day}, "
                   f"storage_type={storage_type}, bucket_name={bucket_name}, html_prefix={html_prefix}, "
                   f"json_prefix={json_prefix}, lookup_type={lookup_type}, force_scrape={force_scrape}")

        # Just call run_month with a single day
        return run_month(year, month, storage_type, bucket_name, html_prefix, json_prefix,
                        lookup_file, lookup_type, region, target_days=[day], table_name=table_name,
                        force_scrape=force_scrape, use_test_data=use_test_data)
    except Exception as e:
        logger.error(f"Error in run_scraper: {str(e)}", exc_info=True)
        raise RuntimeError(f"Error in run_scraper: {str(e)}")


def run_month(year=None, month=None, storage_type='s3', bucket_name=None,
              html_prefix='data/html', json_prefix='data/json', lookup_file='data/lookup.json',
              lookup_type='file', region='us-east-2', target_days=None, table_name=None,
              force_scrape=False, use_test_data=False):
    """Run the scraper for specific days in a month"""
    try:
        logger.info(f"Starting run_month with params: year={year}, month={month}, "
                   f"storage_type={storage_type}, bucket_name={bucket_name}, html_prefix={html_prefix}, "
                   f"json_prefix={json_prefix}, lookup_type={lookup_type}, target_days={target_days}, "
                   f"force_scrape={force_scrape}, use_test_data={use_test_data}")

        # Get the number of days in the month if we need all days
        if target_days is None:
            if month == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month + 1, 1)
            last_day = (next_month - timedelta(days=1)).day
            target_days = range(1, last_day + 1)
        else:
            # Ensure target_days is a list
            target_days = list(target_days)

        logger.info(f"Target days to process: {target_days}")

        success = True
        errors = []

        # Configure logging for Scrapy
        configure_logging()

        # Get bucket name from environment if not provided
        if storage_type == 's3' and not bucket_name:
            bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
            logger.info(f"Using bucket name from environment: {bucket_name}")

        # Create necessary directories if using file storage
        if storage_type == 'file':
            logger.info("Creating necessary directories for file storage")
            os.makedirs(os.path.dirname(html_prefix), exist_ok=True)
            os.makedirs(os.path.dirname(json_prefix), exist_ok=True)
            os.makedirs(os.path.dirname(lookup_file), exist_ok=True)

        # Create a single crawler process with settings
        settings = get_project_settings()
        settings.update({
            'LOG_LEVEL': 'INFO',
            'COOKIES_DEBUG': True,
            'DOWNLOAD_DELAY': 1,
            'CONCURRENT_REQUESTS': 1,
            'TELNETCONSOLE_ENABLED': False  # Disable telnet console for Lambda
        })
        logger.info(f"Using Scrapy settings: {settings.copy_to_dict()}")

        try:
            process = CrawlerProcess(settings)
            logger.info("Created CrawlerProcess successfully")
        except Exception as e:
            logger.error(f"Failed to create CrawlerProcess: {str(e)}", exc_info=True)
            raise

        # Schedule all spiders
        for day in target_days:
            date_str = f"{year}-{month:02d}-{day:02d}"
            logger.info(f"Scheduling scrape for {date_str}")

            try:
                process.crawl(
                    'schedule',
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
                    table_name=table_name or os.environ.get('DYNAMODB_TABLE', 'ncsh-scraped-dates'),
                    force_scrape=force_scrape,
                    use_test_data=use_test_data
                )
                logger.info(f"Successfully scheduled spider for {date_str}")
            except Exception as e:
                logger.error(f"Failed to schedule spider for {date_str}: {str(e)}", exc_info=True)
                raise

        # Create a deferred to track spider completion
        from twisted.internet import defer
        deferreds = []

        # Schedule all spiders and collect their deferreds
        for day in target_days:
            date_str = f"{year}-{month:02d}-{day:02d}"
            logger.info(f"Scheduling scrape for {date_str}")
            d = process.crawl(
                'schedule',
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
                table_name=table_name or os.environ.get('DYNAMODB_TABLE', 'ncsh-scraped-dates'),
                force_scrape=force_scrape,
                use_test_data=use_test_data
            )
            deferreds.append(d)
            logger.info(f"Successfully scheduled spider for {date_str}")

        # Wait for all spiders to complete
        logger.info("Waiting for spiders to complete")
        deferred_list = defer.DeferredList(deferreds)
        deferred_list.addCallback(lambda _: logger.info("All spiders completed"))

        # Start the reactor if not already started
        global _reactor_started
        if not _reactor_started:
            logger.info("Starting Scrapy reactor")
            try:
                process.start()
                _reactor_started = True
                logger.info("Scrapy reactor completed successfully")
            except Exception as e:
                logger.error(f"Error in Scrapy reactor: {str(e)}", exc_info=True)
                raise
        else:
            logger.info("Using existing reactor")
            # Wait a bit for spiders to complete their work
            import time
            time.sleep(5)

        # Verify files were created for all target days
        for day in target_days:
            date_str = f"{year}-{month:02d}-{day:02d}"

            # Define expected files based on storage type and test mode
            prefix = 'test_data' if use_test_data else 'data'
            logger.info(f"Verifying files with prefix: {prefix}")

            expected_files = [
                f"{prefix}/html/{date_str}.html",
                f"{prefix}/json/{date_str}_meta.json",
                f"{prefix}/games/year={year}/month={month:02d}/day={day:02d}/data.jsonl",
                f"{prefix}/metadata/year={year}/month={month:02d}/day={day:02d}/data.jsonl"
            ]
            logger.info(f"Expected files to verify: {expected_files}")

            # Get the storage interface based on configuration
            from ncsoccer.pipeline.config import get_storage_interface
            storage = get_storage_interface(storage_type, bucket_name, region)
            logger.info(f"Using storage interface: {storage.__class__.__name__}")

            # Verify files using the storage interface
            for file_path in expected_files:
                logger.info(f"Checking for file: {file_path}")
                if not storage.exists(file_path):
                    error_msg = f"Expected file {file_path} was not created"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                logger.info(f"Found file: {file_path}")

            logger.info(f"Successfully verified files for {date_str}")

    except Exception as e:
        error_msg = f"Error running scraper: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)
        success = False

    if not success:
        raise RuntimeError(f"Scraper failed with errors: {'; '.join(errors)}")

    return success


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

    args = parser.parse_args()

    # Common parameters
    common_params = {
        'storage_type': args.storage_type,
        'bucket_name': args.bucket_name,
        'html_prefix': args.html_prefix,
        'json_prefix': args.json_prefix,
        'lookup_type': args.lookup_type,
        'table_name': args.table_name,
        'force_scrape': args.force_scrape
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
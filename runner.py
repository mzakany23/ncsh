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
    twisted.internet.asyncio.install()
except (ImportError, Exception) as e:
    print(f"Warning: Could not install asyncio reactor: {e}")

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
             lookup_type='file', region='us-east-2', table_name=None):
    """Run the scraper for a specific day"""
    # Just call run_month with a single day
    return run_month(year, month, storage_type, bucket_name, html_prefix, json_prefix,
                    lookup_file, lookup_type, region, target_days=[day], table_name=table_name)


def run_month(year=None, month=None, storage_type='s3', bucket_name=None,
              html_prefix='data/html', json_prefix='data/json', lookup_file='data/lookup.json',
              lookup_type='file', region='us-east-2', target_days=None, table_name=None):
    """Run the scraper for specific days in a month

    Args:
        year (int): Year to scrape
        month (int): Month to scrape
        storage_type (str): Storage type ('file' or 's3')
        bucket_name (str): S3 bucket name (for s3 storage)
        html_prefix (str): Prefix for HTML files
        json_prefix (str): Prefix for JSON files
        lookup_file (str): Path to lookup file
        lookup_type (str): Lookup type ('file' or 'dynamodb')
        region (str): AWS region name
        target_days (list[int], optional): Specific days to scrape. If None, scrapes all days in month.
        table_name (str, optional): DynamoDB table name (for dynamodb lookup)
    """
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

    success = True

    try:
        # Configure logging for Scrapy
        configure_logging()

        # Get bucket name from environment if not provided
        if storage_type == 's3' and not bucket_name:
            bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')

        # Create necessary directories if using file storage
        if storage_type == 'file':
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
        process = CrawlerProcess(settings)

        # Schedule all spiders
        for day in target_days:
            date_str = f"{year}-{month:02d}-{day:02d}"
            logger.info(f"Scheduling scrape for {date_str}")

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
                table_name=table_name or os.environ.get('DYNAMODB_TABLE', 'ncsh-scraped-dates')
            )

        # Start the reactor once for all spiders
        process.start()

        # Verify files were created for all target days
        for day in target_days:
            date_str = f"{year}-{month:02d}-{day:02d}"
            expected_files = [
                f"{html_prefix}/{date_str}.html",
                f"{json_prefix}/{date_str}.json",
                f"{json_prefix}/{date_str}_meta.json"
            ]

            if storage_type == 'file':
                # Verify local files
                for file_path in expected_files:
                    if not os.path.exists(file_path):
                        raise RuntimeError(f"Expected file {file_path} was not created")
                    if os.path.getsize(file_path) == 0:
                        raise RuntimeError(f"File {file_path} is empty")
            else:
                # Verify S3 files
                import boto3
                from botocore.exceptions import ClientError
                s3 = boto3.client('s3', region_name=region)
                for file_path in expected_files:
                    try:
                        response = s3.head_object(Bucket=bucket_name, Key=file_path)
                        if response['ContentLength'] == 0:
                            raise RuntimeError(f"S3 file {file_path} is empty")
                    except ClientError as e:
                        if e.response['Error']['Code'] == '404':
                            raise RuntimeError(f"Expected S3 file {file_path} was not created")
                        raise

            logger.info(f"Successfully verified files for {date_str}")

    except Exception as e:
        logger.error(f"Error running scraper: {str(e)}")
        success = False

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


def main():
    """Main entry point for the scraper runner."""
    parser = argparse.ArgumentParser(description='Run NC Soccer schedule scraper')
    parser.add_argument('--mode', choices=['day', 'month'], default='day', help='Scraping mode')
    parser.add_argument('--year', type=int, help='Target year')
    parser.add_argument('--month', type=int, help='Target month')
    parser.add_argument('--day', type=int, help='Target day (only for day mode)')
    parser.add_argument('--storage-type', choices=['file', 's3'], default='s3', help='Storage type')
    parser.add_argument('--bucket-name', help='S3 bucket name (only for s3 storage)')
    parser.add_argument('--html-prefix', default='data/html', help='Prefix for HTML files')
    parser.add_argument('--json-prefix', default='data/json', help='Prefix for JSON files')
    parser.add_argument('--lookup-file', default='data/lookup.json', help='Path to lookup file')
    parser.add_argument('--lookup-type', choices=['file', 'dynamodb'], default='file', help='Lookup storage type')
    parser.add_argument('--region', default='us-east-2', help='AWS region name')
    parser.add_argument('--table-name', help='DynamoDB table name (for dynamodb lookup)')

    args = parser.parse_args()

    if args.mode == 'day' and not args.day:
        parser.error('Day is required for day mode')

    if args.mode == 'day':
        run_scraper(args.year, args.month, args.day, args.storage_type, args.bucket_name,
                   args.html_prefix, args.json_prefix, args.lookup_file, args.lookup_type, args.region,
                   args.table_name)
    else:
        run_month(args.year, args.month, args.storage_type, args.bucket_name,
                 args.html_prefix, args.json_prefix, args.lookup_file, args.lookup_type, args.region,
                 table_name=args.table_name)


if __name__ == '__main__':
    exit(main())
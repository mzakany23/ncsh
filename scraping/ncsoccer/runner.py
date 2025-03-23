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
from scrapy import signals
from ncsoccer.spiders.schedule_spider import ScheduleSpider

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
        bool: Success status
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

            # Ensure directories start with /tmp in Lambda to avoid read-only filesystem errors
            if not html_prefix.startswith('/tmp/') and not html_prefix.startswith('s3://'):
                html_prefix = f'/tmp/{html_prefix}'
                logger.info(f"Adjusted html_prefix for Lambda: {html_prefix}")

            if not json_prefix.startswith('/tmp/') and not json_prefix.startswith('s3://'):
                json_prefix = f'/tmp/{json_prefix}'
                logger.info(f"Adjusted json_prefix for Lambda: {json_prefix}")

            if not lookup_file.startswith('/tmp/') and not lookup_file.startswith('s3://'):
                lookup_file = f'/tmp/{lookup_file}'
                logger.info(f"Adjusted lookup_file for Lambda: {lookup_file}")

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
                return True

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

            # Check if date is already scraped
            if not force_scrape and checkpoint.is_date_scraped(date_str):
                logger.info(f"Date {date_str} already scraped according to checkpoint")
                return True

        # Configure Scrapy spider settings
        settings = get_project_settings()
        settings.set('LOG_LEVEL', 'INFO')

        # Skip storing callbacks if using architecture v2
        if architecture_version != 'v2':
            settings.set('ITEM_PIPELINES', {
                'ncsoccer.pipeline.storage.StoragePipeline': 300,
            })

        settings.set('STORAGE_TYPE', storage_type)
        settings.set('S3_BUCKET', bucket_name)
        settings.set('HTML_PREFIX', html_prefix)
        settings.set('JSON_PREFIX', json_prefix)
        settings.set('AWS_REGION', region)
        settings.set('ARCHITECTURE_VERSION', architecture_version)
        settings.set('USE_TEST_DATA', use_test_data)
        settings.set('DOWNLOAD_TIMEOUT', 20)  # 20 seconds to download
        settings.set('DOWNLOAD_DELAY', 1)     # 1 second between requests
        settings.set('RANDOMIZE_DOWNLOAD_DELAY', True)
        settings.set('USER_AGENT', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36')

        # Create crawler process
        process = CrawlerProcess(settings)

        # Use a class variable to track whether spider finished
        class SpiderTracker:
            finished = False
            games_count = 0
            success = False

        # Create the spider
        spider = ScheduleSpider(
            date_str=date_str,
            date_dict={'year': year, 'month': month, 'day': day},
            verbose=True
        )

        # Define callbacks for spider signals
        def spider_closed(spider, reason):
            logger.info(f"Spider closed: {reason}")
            SpiderTracker.finished = True
            if reason == 'finished':
                SpiderTracker.success = True

        def item_scraped(item, response, spider):
            SpiderTracker.games_count += 1
            logger.info(f"Scraped item {SpiderTracker.games_count}: {item.get('home_team')} vs {item.get('away_team')}")

        # Configure crawler with callbacks
        crawler = process.create_crawler(ScheduleSpider)
        crawler.signals.connect(spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(item_scraped, signal=signals.item_scraped)

        # Start the crawler
        process.crawl(crawler, date_str=date_str, date_dict={'year': year, 'month': month, 'day': day}, verbose=True)
        process.start()  # This will block until crawling is finished

        # Wait for the spider to finish
        if not SpiderTracker.finished:
            logger.warning("Spider did not report as finished")

        # Update lookup data
        if lookup_type == 'file' and SpiderTracker.success:
            lookup_data = load_lookup_data(lookup_file, storage_type, bucket_name, region)
            update_lookup_data(lookup_data, date_str, SpiderTracker.success, SpiderTracker.games_count,
                               lookup_file, storage_type, bucket_name, region)

        if checkpoint and SpiderTracker.success:
            checkpoint.update_scraping(date_str, success=True, games_count=SpiderTracker.games_count)
            logger.info(f"Marked {date_str} as scraped in checkpoint file")

        # Verify files were created
        if not skip_wait and storage_type == 's3':
            logger.info(f"Verifying files were created in S3: {date_str}")

            # Import here to avoid circular imports
            from ncsoccer.pipeline.config import get_storage_interface
            storage = get_storage_interface('s3', bucket_name, region)

            # Define file paths to verify
            html_path = f"{html_prefix}/{date_str}.html"
            json_path = f"{json_prefix}/{date_str}.json"

            # Wait for files to appear in S3
            html_exists = wait_for_file(storage, html_path, max_wait)
            json_exists = wait_for_file(storage, json_path, max_wait)

            if html_exists and json_exists:
                logger.info(f"All files verified for {date_str}")
                return True
            else:
                logger.error(f"Not all files were created: HTML:{html_exists} JSON:{json_exists}")
                return False
        elif skip_wait:
            logger.info(f"Skipping file verification for {date_str} (skip_wait=True)")
            return True

        # If no verification needed, return spider success status
        return SpiderTracker.success

    except Exception as e:
        logger.error(f"Error in run_scraper: {str(e)}", exc_info=True)
        raise RuntimeError(f"Error in run_scraper: {str(e)}")


def run_month(year=None, month=None, storage_type='s3', bucket_name=None,
              html_prefix='data/html', json_prefix='data/json', lookup_file='data/lookup.json',
              lookup_type='file', region='us-east-2', target_days=None, table_name=None,
              force_scrape=False, use_test_data=False, max_retries=3, architecture_version='v1',
              max_wait=300):
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
        max_wait (int): Maximum seconds to wait for file creation

    Returns:
        bool: Success status
    """
    retry_count = 0
    last_error = None

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

        # Ensure directories start with /tmp in Lambda to avoid read-only filesystem errors
        if not html_prefix.startswith('/tmp/') and not html_prefix.startswith('s3://'):
            html_prefix = f'/tmp/{html_prefix}'
            logger.info(f"Adjusted html_prefix for Lambda: {html_prefix}")

        if not json_prefix.startswith('/tmp/') and not json_prefix.startswith('s3://'):
            json_prefix = f'/tmp/{json_prefix}'
            logger.info(f"Adjusted json_prefix for Lambda: {json_prefix}")

        if not lookup_file.startswith('/tmp/') and not lookup_file.startswith('s3://'):
            lookup_file = f'/tmp/{lookup_file}'
            logger.info(f"Adjusted lookup_file for Lambda: {lookup_file}")

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
            # Load lookup data using our updated function
            lookup_data = {}
            if not force_scrape and lookup_type == 'file':
                lookup_data = load_lookup_data(lookup_file, storage_type, bucket_name, region)

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

            # Define success callback to track which dates were successful
            successful_dates = {}

            def track_date_success(date_str, success=True, games_count=0):
                """Callback to track successful dates"""
                successful_dates[date_str] = {
                    'success': success,
                    'games_count': games_count
                }

            # Configure Scrapy spider settings
            settings = get_project_settings()
            settings.set('LOG_LEVEL', 'INFO')

            # Skip storing callbacks if using architecture v2
            if architecture_version != 'v2':
                settings.set('ITEM_PIPELINES', {
                    'ncsoccer.pipeline.storage.StoragePipeline': 300,
                })

            settings.set('DOWNLOAD_TIMEOUT', 60)
            settings.set('COOKIES_ENABLED', False)
            settings.set('RETRY_TIMES', 3)

            # Create storage settings
            storage_settings = {
                'STORAGE_TYPE': storage_type,
                'BUCKET_NAME': bucket_name,
                'HTML_PREFIX': html_prefix,
                'JSON_PREFIX': json_prefix,
                'REGION_NAME': region,
            }

            # Apply storage settings
            for key, value in storage_settings.items():
                settings.set(key, value)

            # Create CrawlerProcess
            process = CrawlerProcess(settings)

            # Create spider for each target day
            for day in target_days:
                date_obj = datetime(year, month, day)
                date_str = date_obj.strftime("%Y-%m-%d")

                logger.info(f"Configuring spider for {date_str}")

                # Create crawler
                spider = process.create_crawler('schedule')

                # Configure spider
                spider_args = {
                    'year': year,
                    'month': month,
                    'day': day,
                    'storage_type': storage_type,
                    'bucket_name': bucket_name,
                    'html_prefix': html_prefix,
                    'json_prefix': json_prefix,
                    'architecture_version': architecture_version,
                    'use_test_data': use_test_data
                }

                # Crawl with spider
                try:
                    process.crawl(spider, **spider_args)
                    logger.info(f"Scheduled spider for {date_str}")
                except Exception as e:
                    logger.error(f"Error scheduling spider for {date_str}: {str(e)}")
                    success = False
                    errors.append(f"Error scheduling {date_str}: {str(e)}")

            # Start the crawling process
            try:
                logger.info(f"Starting crawler process for {len(target_days)} days")
                process.start(stop_after_crawl=True)
                logger.info("Crawler process completed")

            except Exception as e:
                logger.error(f"Error in crawler process: {str(e)}", exc_info=True)
                success = False
                errors.append(str(e))

            if not success:
                logger.error(f"Crawl process failed with errors: {errors}")
                retry_count += 1
                last_error = errors
                continue

            # Verify files were created
            logger.info("Verifying files were created")
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

                # Track if all files exist for this day
                all_files_exist = True
                files_verified = 0

                # Wait for files to be created (up to max_wait seconds)
                for file_path in expected_files:
                    wait_start = time.time()
                    file_exists = False

                    while not file_exists and (time.time() - wait_start) < max_wait:
                        if storage.exists(file_path):
                            file_exists = True
                            files_verified += 1
                            logger.info(f"Verified file exists: {file_path}")
                            break
                        else:
                            logger.info(f"Waiting for file to be created: {file_path}")
                            time.sleep(5)

                    if not file_exists:
                        logger.error(f"File not created after {max_wait} seconds: {file_path}")
                        all_files_exist = False

                # Update lookup data for this day
                if all_files_exist:
                    logger.info(f"All files verified for {date_str}. Updating lookup data.")
                    # Get the games count from the meta file
                    games_count = 0
                    try:
                        meta_path = path_manager.get_json_meta_path(date_obj)
                        meta_content = storage.read(meta_path)
                        meta_data = json.loads(meta_content)
                        games_count = meta_data.get('games_count', 0)
                    except Exception as e:
                        logger.error(f"Error reading games count for {date_str}: {e}")

                    # Update lookup data using our new function
                    lookup_data = update_lookup_data(
                        lookup_data, date_str, success=True, games_count=games_count,
                        lookup_file=lookup_file, storage_type=storage_type,
                        bucket_name=bucket_name, region=region
                    )
                else:
                    logger.error(f"Not all files were created for {date_str}")
                    success = False
                    errors.append(f"Missing files for {date_str}")

            if success:
                logger.info(f"Successfully scraped {len(target_days)} days")
                return True

            retry_count += 1
            last_error = errors

        except Exception as e:
            logger.error(f"Error in run_month (attempt {retry_count + 1}): {str(e)}", exc_info=True)
            retry_count += 1
            last_error = str(e)
            time.sleep(2)  # Brief delay before retry

    # If we get here, all retries failed
    logger.error(f"All {max_retries} attempts failed. Last error: {last_error}")
    raise RuntimeError(f"Failed to run month after {max_retries} attempts. Last error: {last_error}")


def run_date_range(start_date, end_date, storage_type='s3', bucket_name=None,
                  html_prefix='data/html', json_prefix='data/json', lookup_file='data/lookup.json',
                  lookup_type='file', region='us-east-2', force_scrape=False, use_test_data=False,
                  architecture_version='v1', max_wait=300):
    """Run scraper for a range of dates.

    Args:
        start_date (datetime): Start date to scrape from.
        end_date (datetime): End date to scrape to.
        storage_type (str): Storage type ('file' or 's3')
        bucket_name (str): S3 bucket name if using S3 storage
        html_prefix (str): Prefix for HTML files
        json_prefix (str): Prefix for JSON files
        lookup_file (str): Path to lookup file
        lookup_type (str): Type of lookup ('file' only supported)
        region (str): AWS region
        force_scrape (bool): Whether to force scrape even if date exists
        use_test_data (bool): Whether to use test data paths
        architecture_version (str): Data architecture version ('v1' or 'v2')
        max_wait (int): Maximum seconds to wait for file creation

    Returns:
        bool: True if all dates were scraped successfully, False otherwise.
    """
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

        # Ensure directories start with /tmp in Lambda to avoid read-only filesystem errors
        if not html_prefix.startswith('/tmp/') and not html_prefix.startswith('s3://'):
            html_prefix = f'/tmp/{html_prefix}'
            logger.info(f"Adjusted html_prefix for Lambda: {html_prefix}")

        if not json_prefix.startswith('/tmp/') and not json_prefix.startswith('s3://'):
            json_prefix = f'/tmp/{json_prefix}'
            logger.info(f"Adjusted json_prefix for Lambda: {json_prefix}")

        if not lookup_file.startswith('/tmp/') and not lookup_file.startswith('s3://'):
            lookup_file = f'/tmp/{lookup_file}'
            logger.info(f"Adjusted lookup_file for Lambda: {lookup_file}")

    current = start_date
    failed_dates = []

    logger.info(f"Starting date range scrape from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        logger.info(f"Processing date: {date_str}")

        # Run the scraper for this specific day
        success = run_scraper(
            year=current.year,
            month=current.month,
            day=current.day,
            storage_type=storage_type,
            bucket_name=bucket_name,
            html_prefix=html_prefix,
            json_prefix=json_prefix,
            lookup_file=lookup_file,
            lookup_type=lookup_type,
            region=region,
            force_scrape=force_scrape,
            use_test_data=use_test_data,
            architecture_version=architecture_version,
            max_wait=max_wait
        )

        if not success:
            failed_dates.append(date_str)
            logger.error(f"Failed to scrape date: {date_str}")
        else:
            logger.info(f"Successfully scraped date: {date_str}")

        current = current + timedelta(days=1)

    if failed_dates:
        logger.error(f"Failed to scrape dates: {failed_dates}")
        return False

    logger.info(f"Successfully completed date range scrape from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    return True


def update_lookup_data(lookup_data, date_str, success=True, games_count=0, lookup_file='data/lookup.json',
                   storage_type='file', bucket_name=None, region='us-east-2'):
    """Update the lookup data with a new date.

    Args:
        lookup_data (dict): The lookup data dictionary.
        date_str (str): Date string in YYYY-MM-DD format.
        success (bool): Whether scraping was successful.
        games_count (int): Number of games found.
        lookup_file (str): Path to the lookup JSON file.
        storage_type (str): 'file' or 's3'
        bucket_name (str): S3 bucket name if storage_type is 's3'
        region (str): AWS region for S3

    Returns:
        dict: Updated lookup data.
    """
    # Detect Lambda environment - if we're in Lambda, ensure we use S3
    in_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
    if in_lambda and storage_type == 'file':
        logger.warning("Running in Lambda environment - forcing S3 storage type")
        storage_type = 's3'
        # Get bucket name from environment if not provided and we're in Lambda
        if not bucket_name:
            bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')

    # Update the lookup data
    lookup_data[date_str] = {
        'success': success,
        'games_count': games_count,
        'timestamp': datetime.now().isoformat()
    }

    try:
        # Prepare the full data object
        data = {'scraped_dates': lookup_data}

        if storage_type == 's3':
            # Import here to avoid circular imports
            from ncsoccer.pipeline.config import get_storage_interface
            # Get S3 storage interface
            storage = get_storage_interface('s3', bucket_name, region)
            # Write to S3
            storage.write(lookup_file, json.dumps(data, indent=2))
            logger.info(f"Updated lookup data in S3: {bucket_name}/{lookup_file}")
        else:
            # Local file system - use /tmp in Lambda
            lambda_tmp_prefix = '/tmp/' if in_lambda else ''
            local_lookup_file = f"{lambda_tmp_prefix}{lookup_file}"

            os.makedirs(os.path.dirname(local_lookup_file), exist_ok=True)
            with open(local_lookup_file, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Updated lookup data in local file: {local_lookup_file}")
    except Exception as e:
        logger.error(f"Error updating lookup data: {e}")

    return lookup_data


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

    print(f"Scraper {'succeeded' if result else 'failed'}")
    import sys
    sys.exit(0 if result else 1)
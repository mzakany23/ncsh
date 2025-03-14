#!/usr/bin/env python3
"""
Efficient backfill runner script for NC Soccer data.

This script runs the BackfillSpider which navigates through months in reverse chronological order,
maintaining the browser session between months to minimize navigation steps. This is much more
efficient than the traditional approach of starting from scratch for each historical month.
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

# Try to install asyncio reactor for improved performance
try:
    import asyncio
    import twisted.internet.asyncio
    from twisted.internet import reactor
    twisted.internet.asyncio.install()
except (ImportError, Exception) as e:
    logger.warning(f"Could not install asyncio reactor: {e}")
    # Fall back to standard reactor
    try:
        from twisted.internet import reactor
    except ImportError:
        logger.error("Could not import any reactor from Twisted")

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

def run_backfill(start_year=2007, start_month=1, end_year=None, end_month=None,
                 storage_type='s3', bucket_name=None, html_prefix='data/html',
                 json_prefix='data/json', lookup_file='data/lookup.json',
                 lookup_type='dynamodb', region='us-east-2', table_name=None,
                 force_scrape=False, timeout=900):
    """
    Run the backfill spider.
    
    Args:
        start_year (int): Oldest year to scrape (inclusive)
        start_month (int): Oldest month to scrape (inclusive)
        end_year (int): Newest year to scrape (inclusive), defaults to current year
        end_month (int): Newest month to scrape (inclusive), defaults to current month
        storage_type (str): 'file' or 's3'
        bucket_name (str): S3 bucket name if storage_type is 's3'
        html_prefix (str): Prefix for HTML files
        json_prefix (str): Prefix for JSON files
        lookup_file (str): Path to lookup file
        lookup_type (str): 'file' or 'dynamodb'
        region (str): AWS region
        table_name (str): DynamoDB table name if lookup_type is 'dynamodb'
        force_scrape (bool): Whether to re-scrape already scraped dates
        timeout (int): Maximum time in seconds to run before checkpointing
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Starting backfill spider")
    
    # Set defaults for end date if not provided
    now = datetime.now()
    if end_year is None:
        end_year = now.year
    if end_month is None:
        end_month = now.month
        
    # Get bucket name from environment if not provided
    if storage_type == 's3' and not bucket_name:
        bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
        logger.info(f"Using bucket name from environment: {bucket_name}")
        
    # Table name from environment if not provided
    if lookup_type == 'dynamodb' and not table_name:
        table_name = os.environ.get('DYNAMODB_TABLE', 'ncsh-scraped-dates')
        logger.info(f"Using table name from environment: {table_name}")
    
    # Configure Scrapy settings
    settings = get_project_settings()
    settings.update({
        'LOG_LEVEL': 'INFO',
        'COOKIES_DEBUG': True,
        'DOWNLOAD_DELAY': 1,  # Be polite to the server
        'CONCURRENT_REQUESTS': 1,  # Sequential processing for session consistency
        'TELNETCONSOLE_ENABLED': False,  # Disable for AWS Lambda
        'DOWNLOAD_TIMEOUT': 120,  # Longer timeout for slow ASP.NET pages
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429]
    })
    
    try:
        # Create process
        process = CrawlerProcess(settings)
        
        # Create checkpoint filename
        checkpoint_file = f"{json_prefix}/backfill_checkpoint_{start_year}_{start_month}_{end_year}_{end_month}.json"
        
        # Import the backfill spider directly
        from ncsoccer.spiders.backfill_spider import BackfillSpider
        
        # Schedule the spider using the class directly
        process.crawl(
            'backfill_spider',
            start_year=start_year,
            start_month=start_month,
            end_year=end_year,
            end_month=end_month,
            storage_type=storage_type,
            bucket_name=bucket_name,
            html_prefix=html_prefix,
            json_prefix=json_prefix,
            lookup_file=lookup_file,
            lookup_type=lookup_type,
            region=region,
            table_name=table_name,
            force_scrape=force_scrape,
            checkpoint_file=checkpoint_file
        )
        
        # Set a timeout to avoid Lambda timeouts
        start_time = time.time()
        
        # Define a function to stop the crawler after timeout
        def check_timeout():
            if time.time() - start_time > timeout:
                logger.info(f"Reached timeout of {timeout} seconds, stopping crawler")
                # This will gracefully stop the crawler
                reactor.callFromThread(reactor.stop)
            else:
                # Check again in 10 seconds
                reactor.callLater(10, check_timeout)
        
        # Start timeout checking
        reactor.callLater(10, check_timeout)
        
        # Start the process
        process.start()
        
        # Check if we timed out or completed successfully
        elapsed = time.time() - start_time
        if elapsed >= timeout:
            logger.info("Crawler stopped due to timeout - will resume from checkpoint next run")
            return True  # We'll consider this a success and continue from checkpoint next time
        else:
            logger.info(f"Crawler completed successfully in {elapsed:.2f} seconds")
            return True
            
    except Exception as e:
        logger.error(f"Error running backfill: {str(e)}", exc_info=True)
        return False

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    try:
        # Extract parameters from event with defaults
        start_year = event.get('start_year', 2007)
        start_month = event.get('start_month', 1)
        end_year = event.get('end_year')
        end_month = event.get('end_month')
        force_scrape = event.get('force_scrape', False)
        
        # Get bucket name and table name from environment variables
        bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
        table_name = os.environ.get('DYNAMODB_TABLE', 'ncsh-scraped-dates')
        
        # Calculate an appropriate timeout - leave 30 seconds for cleanup
        max_lambda_time = context.get_remaining_time_in_millis() if context else 900000
        timeout = (max_lambda_time / 1000) - 30  # Convert to seconds and leave margin
        
        result = run_backfill(
            start_year=start_year,
            start_month=start_month,
            end_year=end_year,
            end_month=end_month,
            storage_type='s3',
            bucket_name=bucket_name,
            lookup_type='dynamodb',
            table_name=table_name,
            force_scrape=force_scrape,
            timeout=timeout
        )
        
        return {"statusCode": 200, "body": json.dumps({"result": result})}
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NC Soccer Backfill Spider')
    parser.add_argument('--start-year', type=int, default=2007, help='Oldest year to scrape')
    parser.add_argument('--start-month', type=int, default=1, help='Oldest month to scrape')
    parser.add_argument('--end-year', type=int, help='Newest year to scrape')
    parser.add_argument('--end-month', type=int, help='Newest month to scrape')
    parser.add_argument('--force-scrape', action='store_true', help='Force re-scrape already scraped dates')
    parser.add_argument('--storage-type', choices=['file', 's3'], default='s3', help='Storage type')
    parser.add_argument('--bucket-name', help='S3 bucket name')
    parser.add_argument('--lookup-type', choices=['file', 'dynamodb'], default='dynamodb', help='Lookup type')
    parser.add_argument('--table-name', help='DynamoDB table name')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--timeout', type=int, default=900, help='Maximum time to run in seconds')
    
    args = parser.parse_args()
    
    # Run the backfill
    result = run_backfill(
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month,
        storage_type=args.storage_type,
        bucket_name=args.bucket_name,
        lookup_type=args.lookup_type,
        table_name=args.table_name,
        region=args.region,
        force_scrape=args.force_scrape,
        timeout=args.timeout
    )
    
    # Print result and exit with appropriate code
    print(json.dumps({"result": result}))
    sys.exit(0 if result else 1)
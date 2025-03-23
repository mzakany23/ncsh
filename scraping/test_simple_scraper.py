#!/usr/bin/env python3
"""Test script for the simplified scraper without Scrapy."""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
import argparse
from ncsoccer.scraper import SimpleScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    """Run the test script."""
    parser = argparse.ArgumentParser(description='Test the SimpleScraper implementation')
    parser.add_argument('--mode', choices=['day', 'range'], default='day', help='Scrape mode')
    parser.add_argument('--year', type=int, help='Year to scrape (for day mode)')
    parser.add_argument('--month', type=int, help='Month to scrape (for day mode)')
    parser.add_argument('--day', type=int, help='Day to scrape (for day mode)')
    parser.add_argument('--start-date', type=str, help='Start date for range mode (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date for range mode (YYYY-MM-DD)')
    parser.add_argument('--force-scrape', action='store_true', help='Force scrape even if already done')
    parser.add_argument('--storage-type', choices=['file', 's3'], default='file', help='Storage type')
    parser.add_argument('--bucket-name', default='ncsh-app-data', help='S3 bucket name')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--architecture-version', choices=['v1', 'v2'], default='v1', help='Architecture version')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of worker threads')
    parser.add_argument('--timeout', type=int, default=30, help='Request timeout in seconds')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum retry attempts')

    args = parser.parse_args()

    # Get current date for defaults
    now = datetime.now()

    if args.mode == 'day':
        # Single day mode
        year = args.year or now.year
        month = args.month or now.month
        day = args.day or now.day

        logger.info(f"Running in day mode for {year}-{month:02d}-{day:02d}")

        scraper = SimpleScraper(
            mode='day',
            year=year,
            month=month,
            day=day,
            storage_type=args.storage_type,
            bucket_name=args.bucket_name,
            region=args.region,
            force_scrape=args.force_scrape,
            architecture_version=args.architecture_version,
            max_workers=args.max_workers,
            timeout=args.timeout,
            max_retries=args.max_retries
        )

        start_time = time.time()
        success = scraper.run()
        duration = time.time() - start_time

        logger.info(f"Scraping {'succeeded' if success else 'failed'} in {duration:.2f} seconds")
        logger.info(f"Scraped {scraper.games_scraped} games")

    else:
        # Date range mode
        if args.start_date and args.end_date:
            try:
                start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
                end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
            except ValueError as e:
                logger.error(f"Invalid date format: {e}. Use YYYY-MM-DD format.")
                return 1
        else:
            # Default to current month
            start_date = datetime(now.year, now.month, 1).date()
            if now.month == 12:
                end_date = datetime(now.year, 12, 31).date()
            else:
                end_date = (datetime(now.year, now.month + 1, 1) - timedelta(days=1)).date()

        logger.info(f"Running in range mode from {start_date} to {end_date}")

        scraper = SimpleScraper(
            mode='range',
            start_year=start_date.year,
            start_month=start_date.month,
            start_day=start_date.day,
            end_year=end_date.year,
            end_month=end_date.month,
            end_day=end_date.day,
            storage_type=args.storage_type,
            bucket_name=args.bucket_name,
            region=args.region,
            force_scrape=args.force_scrape,
            architecture_version=args.architecture_version,
            max_workers=args.max_workers,
            timeout=args.timeout,
            max_retries=args.max_retries
        )

        start_time = time.time()
        results = scraper.scrape_date_range(start_date, end_date)
        duration = time.time() - start_time

        # Calculate statistics
        total_dates = (end_date - start_date).days + 1
        success_count = sum(1 for success in results.values() if success)

        logger.info(f"Scraping completed in {duration:.2f} seconds")
        logger.info(f"Successfully scraped {success_count}/{total_dates} dates")
        logger.info(f"Scraped {scraper.games_scraped} games total")

    return 0

if __name__ == "__main__":
    sys.exit(main())
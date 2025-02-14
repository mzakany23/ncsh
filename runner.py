#!/usr/bin/env python3
"""Runner script for soccer schedule scraper with lookup functionality."""

import os
import json
import argparse
import logging
import subprocess
from datetime import datetime, timedelta
from calendar import monthrange

logging.basicConfig(level=logging.INFO)
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


def run_scraper(year, month, day, lookup_file='data/lookup.json', skip_existing=True):
    """Run the scrapy spider for a specific date.

    Args:
        year (int): Year to scrape.
        month (int): Month to scrape.
        day (int): Day to scrape.
        lookup_file (str): Path to the lookup JSON file.
        skip_existing (bool): Whether to skip already scraped dates.

    Returns:
        bool: True if scraping was successful, False otherwise.
    """
    date_str = f"{year}-{month:02d}-{day:02d}"

    # Check lookup data if skipping existing
    if skip_existing:
        lookup_data = load_lookup_data(lookup_file)
        if is_date_scraped(date_str, lookup_data):
            logger.info(f"Skipping {date_str} (already scraped)")
            return True

    # Run the spider
    cmd = [
        'scrapy', 'crawl', 'schedule',
        '-a', f'year={year}',
        '-a', f'month={month}',
        '-a', f'day={day}',
        '-a', f'lookup_file={lookup_file}',
        '-L', 'INFO'  # Set log level to INFO
    ]

    try:
        logger.info(f"Starting scrape for {date_str}")
        # Run the spider and stream output in real-time
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )

        # Stream the output
        for line in process.stdout:
            print(line, end='')  # Print in real-time

        # Wait for completion and get return code
        return_code = process.wait()

        if return_code != 0:
            logger.error(f"Spider failed with return code {return_code}")
            return False

        # Verify files were created
        html_file = f"data/html/{date_str}.html"
        json_file = f"data/json/{date_str}.json"
        meta_file = f"data/json/{date_str}_meta.json"

        files_exist = all(os.path.exists(f) for f in [html_file, json_file, meta_file])
        if not files_exist:
            logger.error(f"Not all files were created for {date_str}")
            return False

        logger.info(f"All files created successfully for {date_str}")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to scrape {date_str}: {e}")
        logger.error(f"Output: {e.output}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while scraping {date_str}: {e}")
        return False


def run_month(year, month, lookup_file='data/lookup.json', skip_existing=True):
    """Run scraper for all days in a month.

    Args:
        year (int): Year to scrape.
        month (int): Month to scrape.
        lookup_file (str): Path to the lookup JSON file.
        skip_existing (bool): Whether to skip already scraped dates.

    Returns:
        bool: True if all days were scraped successfully, False otherwise.
    """
    _, last_day = monthrange(year, month)
    failed_days = []

    for day in range(1, last_day + 1):
        if not run_scraper(year, month, day, lookup_file, skip_existing):
            failed_days.append(day)

    if failed_days:
        logger.error(f"Failed to scrape days: {failed_days}")
        return False
    return True


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
        if not run_scraper(current.year, current.month, current.day,
                          lookup_file, skip_existing):
            failed_dates.append(current.strftime('%Y-%m-%d'))
        current = current + timedelta(days=1)

    if failed_dates:
        logger.error(f"Failed to scrape dates: {failed_dates}")
        return False
    return True


def main():
    """Main entry point for the scraper runner."""
    parser = argparse.ArgumentParser(description='Soccer schedule scraper runner')
    parser.add_argument(
        '--mode',
        choices=['day', 'month', 'range'],
        required=True,
        help='Scraping mode'
    )
    parser.add_argument(
        '--year',
        type=int,
        required=True,
        help='Year to scrape'
    )
    parser.add_argument(
        '--month',
        type=int,
        help='Month to scrape (required for day and month modes)'
    )
    parser.add_argument(
        '--day',
        type=int,
        help='Day to scrape (required for day mode)'
    )
    parser.add_argument(
        '--end-year',
        type=int,
        help='End year for range mode'
    )
    parser.add_argument(
        '--end-month',
        type=int,
        help='End month for range mode'
    )
    parser.add_argument(
        '--end-day',
        type=int,
        help='End day for range mode'
    )
    parser.add_argument(
        '--lookup-file',
        default='data/lookup.json',
        help='Path to lookup file'
    )
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Skip already scraped dates'
    )

    args = parser.parse_args()

    # Validate arguments based on mode
    if args.mode == 'day':
        if not all([args.month, args.day]):
            parser.error("Month and day are required for day mode")
        success = run_scraper(
            args.year,
            args.month,
            args.day,
            args.lookup_file,
            args.skip_existing
        )

    elif args.mode == 'month':
        if not args.month:
            parser.error("Month is required for month mode")
        success = run_month(
            args.year,
            args.month,
            args.lookup_file,
            args.skip_existing
        )

    elif args.mode == 'range':
        if not all([args.end_year, args.end_month, args.end_day]):
            parser.error("End year, month, and day are required for range mode")
        try:
            start_date = datetime(args.year, args.month or 1, args.day or 1)
            end_date = datetime(args.end_year, args.end_month, args.end_day)
            success = run_date_range(
                start_date,
                end_date,
                args.lookup_file,
                args.skip_existing
            )
        except ValueError as e:
            parser.error(f"Invalid date: {e}")

    return 0 if success else 1


if __name__ == '__main__':
    exit(main())
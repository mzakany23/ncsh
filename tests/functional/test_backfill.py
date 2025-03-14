#!/usr/bin/env python3
"""
Comprehensive test script to verify the backfill spider works correctly.
This script:
1. Runs the backfill spider for a specified date range
2. Verifies the data was properly scraped
3. Checks for missing dates
4. Analyzes the results
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
import json
from collections import defaultdict
import time

# Add the scraping directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scraping'))

# Import the backfill spider directly
from ncsoccer.spiders.backfill_spider import BackfillSpider
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("backfill_test.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def verify_data(output_dir, start_year, start_month, end_year, end_month):
    """Verify the data was properly scraped by checking the output directory.
    
    Args:
        output_dir (str): Directory containing scraped data
        start_year (int): Start year
        start_month (int): Start month
        end_year (int): End year
        end_month (int): End month
        
    Returns:
        tuple: (success, stats_dict, missing_dates)
    """
    logger.info("Verifying scraped data...")
    
    # Calculate the date range
    start_date = datetime(start_year, start_month, 1)
    if end_month == 12:
        end_date = datetime(end_year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(end_year, end_month + 1, 1) - timedelta(days=1)
    
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    found_days = 0
    missing_dates = []
    error_dates = []
    
    # Statistics counters
    games_by_month = defaultdict(int)
    games_by_year = defaultdict(int)
    
    logger.info(f"Checking data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({total_days} days)")
    
    # Check each date in the range
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        json_file = os.path.join(output_dir, 'json', f"{date_str}.json")
        meta_file = os.path.join(output_dir, 'json', f"{date_str}_meta.json")
        
        if not os.path.exists(json_file) or not os.path.exists(meta_file):
            logger.warning(f"Missing data for {date_str}")
            missing_dates.append(date_str)
        else:
            # Read the JSON data to verify it
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    
                # Check if data is valid
                if isinstance(data, list):
                    game_count = len(data)
                    games_by_month[f"{current_date.year}-{current_date.month:02d}"] += game_count
                    games_by_year[current_date.year] += game_count
                    logger.info(f"Found {game_count} games for {date_str}")
                    found_days += 1
                else:
                    logger.error(f"Invalid data format for {date_str}: {type(data)}")
                    error_dates.append(date_str)
            except Exception as e:
                logger.error(f"Error reading data for {date_str}: {e}")
                error_dates.append(date_str)
        
        current_date += timedelta(days=1)
    
    # Calculate success metrics
    success_rate = (found_days / total_days) * 100 if total_days > 0 else 0
    logger.info(f"Found data for {found_days} out of {total_days} days ({success_rate:.2f}%)")
    logger.info(f"Missing dates: {len(missing_dates)}")
    logger.info(f"Error dates: {len(error_dates)}")
    
    # Detailed statistics
    logger.info("Games by year:")
    for year, count in sorted(games_by_year.items()):
        logger.info(f"  {year}: {count} games")
    
    logger.info("Games by month:")
    for month, count in sorted(games_by_month.items()):
        logger.info(f"  {month}: {count} games")
    
    stats = {
        "total_days": total_days,
        "found_days": found_days,
        "success_rate": success_rate,
        "missing_dates": len(missing_dates),
        "error_dates": len(error_dates),
        "games_by_year": dict(games_by_year),
        "games_by_month": dict(games_by_month)
    }
    
    # Return overall success (at least 80% of days found)
    return (success_rate >= 80, stats, missing_dates)

def run_backfill(start_year, start_month, end_year, end_month, 
                force_scrape=True, output_dir='data', verify=True,
                skip_existing=False):
    """Run the backfill spider for the specified date range.
    
    Args:
        start_year (int): Start year
        start_month (int): Start month
        end_year (int): End year
        end_month (int): End month
        force_scrape (bool): Whether to force re-scrape even if already scraped
        output_dir (str): Output directory for scraped data
        verify (bool): Whether to verify the scraped data
        skip_existing (bool): Whether to skip already scraped dates
        
    Returns:
        bool: Success status
    """
    # Create output directories
    os.makedirs(os.path.join(output_dir, 'html'), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'json'), exist_ok=True)
    
    logger.info(f"Running backfill from {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
    logger.info(f"Force scrape: {force_scrape}, Skip existing: {skip_existing}")
    
    # Configure Scrapy settings
    settings = get_project_settings()
    settings.update({
        'LOG_LEVEL': 'INFO',
        'COOKIES_DEBUG': True,
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 1,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3
    })
    
    try:
        # Create crawler process
        process = CrawlerProcess(settings)
        
        # Start crawling with the BackfillSpider
        process.crawl(
            BackfillSpider,
            start_year=start_year,
            start_month=start_month,
            end_year=end_year,
            end_month=end_month,
            storage_type='file',  # Use local files for testing
            html_prefix=os.path.join(output_dir, 'html'),
            json_prefix=os.path.join(output_dir, 'json'),
            lookup_file=os.path.join(output_dir, 'lookup.json'),
            lookup_type='file',
            skip_existing=skip_existing,
            force_scrape=force_scrape
        )
        
        # Start the crawler and measure time
        logger.info("Starting crawler...")
        start_time = time.time()
        process.start()
        elapsed_time = time.time() - start_time
        logger.info(f"Crawler finished in {elapsed_time:.2f} seconds")
        
        # Verify the data if requested
        if verify:
            success, stats, missing_dates = verify_data(
                output_dir, start_year, start_month, end_year, end_month
            )
            
            # Write stats to file
            with open(os.path.join(output_dir, 'backfill_stats.json'), 'w') as f:
                json.dump(stats, f, indent=2)
            
            # Write missing dates to file if any
            if missing_dates:
                with open(os.path.join(output_dir, 'missing_dates.json'), 'w') as f:
                    json.dump(missing_dates, f, indent=2)
            
            return success
        
        return True
        
    except Exception as e:
        logger.error(f"Error running backfill: {e}", exc_info=True)
        return False

def main():
    """Main function to parse arguments and run the backfill test."""
    parser = argparse.ArgumentParser(description='Test the backfill spider for NC Soccer data')
    parser.add_argument('--start-year', type=int, default=int(os.environ.get('START_YEAR', '2007')), 
                        help='Start year (default: 2007 or from START_YEAR env var)')
    parser.add_argument('--start-month', type=int, default=int(os.environ.get('START_MONTH', '1')), 
                        help='Start month (default: 1 or from START_MONTH env var)')
    parser.add_argument('--end-year', type=int, default=int(os.environ.get('END_YEAR', str(datetime.now().year))), 
                        help='End year (default: current year or from END_YEAR env var)')
    parser.add_argument('--end-month', type=int, default=int(os.environ.get('END_MONTH', str(datetime.now().month))), 
                        help='End month (default: current month or from END_MONTH env var)')
    parser.add_argument('--force-scrape', action='store_true', default=os.environ.get('FORCE_SCRAPE', '').lower() == 'true', 
                        help='Force re-scrape even if already done')
    parser.add_argument('--skip-existing', action='store_true', help='Skip already scraped dates')
    parser.add_argument('--output-dir', default=os.environ.get('OUTPUT_DIR', 'data'), 
                        help='Output directory for scraped data')
    parser.add_argument('--no-verify', action='store_true', help='Skip verification step')
    
    args = parser.parse_args()
    
    # Run the backfill
    success = run_backfill(
        args.start_year,
        args.start_month,
        args.end_year,
        args.end_month,
        args.force_scrape,
        args.output_dir,
        not args.no_verify,
        args.skip_existing
    )
    
    logger.info(f"Backfill {'successful' if success else 'failed'}")
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
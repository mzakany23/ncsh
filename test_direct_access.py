#!/usr/bin/env python
"""
Test script to validate direct URL access method for historical data scraping
Tests scraping for the last three years with conservative rate limiting
"""

import os
import sys
import datetime
import logging
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

# Add the scraping directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scraping'))

# Import the BackfillSpider
from ncsoccer.spiders.backfill_spider import BackfillSpider

# Configure logging
configure_logging(install_root_handler=False)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('test_direct_access.log')
    ]
)
logger = logging.getLogger(__name__)

def run_test():
    """Run a test of the direct URL access method"""
    # Create test periods - one month from each of the last three years
    current_date = datetime.datetime.now()
    
    # Define test periods - one month from each of the last three years with limited days
    test_periods = [
        {"year": current_date.year - 3, "month": 3, "days": 5},  # 5 days from March three years ago
        {"year": current_date.year - 2, "month": 7, "days": 3},  # 3 days from July two years ago
        {"year": current_date.year - 1, "month": 11, "days": 4}   # 4 days from November last year
    ]
    
    logger.info(f"Testing backfill spider for selected periods in the last three years")
    
    # Get the Scrapy settings
    settings = get_project_settings()
    
    # Override settings for testing - conservative rate limiting
    test_settings = {
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1, 
        'DOWNLOAD_DELAY': 3,  # 3 seconds between requests
        'LOG_LEVEL': 'INFO',
        'COOKIES_DEBUG': True,
        'TELNETCONSOLE_ENABLED': False,
        'RETRY_TIMES': 3,
        'CLOSESPIDER_PAGECOUNT': 100  # Limit to 100 pages for testing 
    }
    
    # Apply test settings
    settings.update(test_settings)
    
    # Create the Scrapy process
    process = CrawlerProcess(settings)
    
    # Run tests for each period
    for period in test_periods:
        year = period["year"]
        month = period["month"]
        days = period["days"]
        
        logger.info(f"Testing period: {year}-{month} (first {days} days)")
        
        # Start the crawler with the backfill spider for this period
        process.crawl(
            BackfillSpider,
            start_year=year,
            start_month=month,
            start_day=1,
            end_year=year,
            end_month=month,
            end_day=days,  # Just test the first few days of each month
            force_scrape=True,
            storage_type='file',
            skip_existing=False  # Force scrape all dates in our test set
        )
    
    # Run the process
    process.start()
    
    logger.info("Test completed successfully")

if __name__ == "__main__":
    logger.info("Starting direct URL access method test")
    run_test()

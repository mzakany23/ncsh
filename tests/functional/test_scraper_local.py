#!/usr/bin/env python3
"""
Simple standalone script to test the scraper functionality locally.
This script directly calls the schedule spider and saves results locally.
"""
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Ensure the correct paths are in sys.path
# Add the project root and scraping directories to the Python path
project_root = Path(__file__).parent.parent.parent
scraping_dir = project_root / 'scraping'

# Add paths to sys.path if they're not already there
for path in [str(project_root), str(scraping_dir)]:
    if path not in sys.path:
        sys.path.insert(0, path)

# Import Scrapy components
try:
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings
    from ncsoccer.spiders.schedule_spider import ScheduleSpider
    from ncsoccer.pipeline.html_to_json import HTMLParser
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error("Make sure you've installed all required dependencies (pip install -r requirements.txt)")
    logger.error("And that you're running this script from the project root directory")
    sys.exit(1)

def run_test_scraper(year=None, month=None, day=None, output_dir='./local_test_output'):
    """
    Run a test scrape for a specific date and save results locally.
    
    Args:
        year (int): Year to scrape (defaults to current year)
        month (int): Month to scrape (defaults to current month)
        day (int): Day to scrape (defaults to current day)
        output_dir (str): Directory to save output files
    
    Returns:
        bool: True if scrape was successful, False otherwise
    """
    # Use current date if not specified
    now = datetime.now()
    year = year or now.year
    month = month or now.month
    day = day or now.day
    
    date_str = f"{year}-{month:02d}-{day:02d}"
    logger.info(f"Testing scraper for date {date_str}")
    
    # Create output directories
    html_dir = Path(output_dir) / 'html'
    json_dir = Path(output_dir) / 'json'
    html_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize settings
    settings = get_project_settings()
    settings.set('LOG_LEVEL', 'INFO')
    settings.set('ITEM_PIPELINES', {'ncsoccer.pipeline.html_to_json.HtmlToJsonPipeline': 300})
    
    # Set up file paths for output
    html_file = html_dir / f"{date_str}.html"
    json_file = json_dir / f"{date_str}.json"
    
    # Create crawler process with custom settings
    process = CrawlerProcess(settings)
    
    # Create spider instance with appropriate parameters
    spider = ScheduleSpider(
        year=year,
        month=month,
        day=day,
        output_file=str(html_file),
        storage_type='file'
    )
    
    # Custom parser for HTML to JSON conversion
    html_parser = HTMLParser(year=year, month=month)
    
    # Set up custom data processing
    def process_data(data, spider):
        # Save HTML data
        with open(html_file, 'w') as f:
            f.write(data)
        
        # Process HTML to JSON
        json_data = html_parser.parse_daily_schedule(data, f"{year}-{month:02d}-{day:02d}")
        
        # Save JSON data
        with open(json_file, 'w') as f:
            json.dump(json_data, f, indent=2)
        
        # Save metadata
        meta_file = json_dir / f"{date_str}_meta.json"
        meta_data = {
            'scrape_time': datetime.now().isoformat(),
            'source': 'local_test',
            'date': date_str
        }
        with open(meta_file, 'w') as f:
            json.dump(meta_data, f, indent=2)
        
        logger.info(f"Scraped {len(json_data)} games for {date_str}")
        return True
    
    # Attach the processor to the spider
    spider.process_data = process_data
    
    try:
        # Run the spider
        process.crawl(spider)
        process.start()  # This will block until the crawl is finished
        
        # Check if files were created
        success = json_file.exists() and html_file.exists()
        
        if success:
            logger.info(f"Scrape successful! Output saved to {json_file}")
            return True
        else:
            logger.error("Scrape failed: output files not created")
            return False
    
    except Exception as e:
        logger.error(f"Error running scraper: {str(e)}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test NC Soccer scraper locally")
    parser.add_argument("--year", type=int, default=datetime.now().year,
                        help="Year to scrape (default: current year)")
    parser.add_argument("--month", type=int, default=datetime.now().month,
                        help="Month to scrape (default: current month)")
    parser.add_argument("--day", type=int, default=datetime.now().day,
                        help="Day to scrape (default: current day)")
    parser.add_argument("--output", type=str, default="./local_test_output",
                        help="Output directory for scraped data")
    
    args = parser.parse_args()
    
    success = run_test_scraper(
        year=args.year,
        month=args.month,
        day=args.day,
        output_dir=args.output
    )
    
    if success:
        logger.info("Scraper test completed successfully!")
    else:
        logger.error("Scraper test failed.")
        sys.exit(1)

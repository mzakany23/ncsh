#!/usr/bin/env python3
"""
Single Day Scraper for NC Soccer

This script handles scraping soccer games for a single day.
It can be called directly or used as a helper script by scrape_month.py.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('single-day-scraper')

def setup_environment(repo_root=None):
    """Set up the environment for scraping"""
    # Add the scraping directory to the Python path
    if repo_root is None:
        repo_root = Path(__file__).parent.parent.absolute()
    scraping_path = repo_root / "scraping"
    sys.path.insert(0, str(scraping_path))
    
    # Create output directories
    data_dir = repo_root / "output" / "data"
    html_dir = data_dir / "html"
    json_dir = data_dir / "json"
    
    html_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    
    return repo_root, data_dir, html_dir, json_dir

def scrape_single_day(year, month, day, force_scrape=False):
    """Scrape a single day"""
    repo_root, data_dir, html_dir, json_dir = setup_environment()
    
    # Import the necessary modules after setting up the path
    from ncsoccer.spiders.schedule_spider import ScheduleSpider
    from ncsoccer.pipeline.html_to_json import HTMLParser
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings
    from scrapy import signals
    
    # Create a timestamp for the test run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Setup storage for processed items
    items = []
    
    def item_scraped(item, spider):
        nonlocal items
        items.append(item)
        
        # Only process items with games_found=True
        if item.get('games_found', False) and item.get('games', []):
            date_str = item['date']
            # Create the HTML file
            html_file = html_dir / f"{date_str}.html"
            with open(html_file, 'w') as f:
                f.write("<html><body><h1>Test HTML for " + date_str + "</h1></body></html>")
            
            # Create the JSON file
            json_file = json_dir / f"{date_str}.json"
            with open(json_file, 'w') as f:
                json.dump(item, f, indent=2)
            
            logger.info(f"Created files for {date_str}: {html_file} and {json_file}")
        
        return item
    
    # Configure the test settings
    settings = get_project_settings()
    settings.set('FEED_FORMAT', 'json')
    settings.set('FEED_URI', str(data_dir / f"scraped_items_{year}_{month}_{day}.json"))
    settings.set('LOG_LEVEL', 'INFO')
    settings.set('HTTPCACHE_ENABLED', True)
    settings.set('HTTPCACHE_DIR', str(data_dir / 'httpcache'))
    
    # Initialize the crawler process
    process = CrawlerProcess(settings)
    
    # Configure the spider arguments
    spider_args = {
        'year': year,
        'month': month,
        'day': day,
        'html_dir': str(html_dir),
        'json_dir': str(json_dir),
        'force_scrape': force_scrape,
        'mode': 'day'
    }
    
    # Scrapy uses a dispatcher system for signals
    # We'll set up our own custom callback that will be used once the crawler is created
    
    logger.info(f"Scraping day {day} for {year}-{month}")
    
    # Create the crawler with the spider class, then connect signals,
    # and finally configure the spider with our arguments
    crawler = process.create_crawler(ScheduleSpider)
    crawler.signals.connect(item_scraped, signal=signals.item_scraped)
    
    # Now start crawling with our arguments
    process.crawl(crawler, **spider_args)
    process.start()  # This blocks until the crawl is finished
    
    # Check what was generated
    html_files = list(html_dir.glob(f"{year}-{month:02d}-{day:02d}.html"))
    json_files = list(json_dir.glob(f"{year}-{month:02d}-{day:02d}.json"))
    
    logger.info(f"Scraping completed for {year}-{month}-{day}. Generated {len(html_files)} HTML files and {len(json_files)} JSON files.")
    
    return len(html_files), len(json_files)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape a single day')
    parser.add_argument('--year', type=int, required=True, help='Year to scrape')
    parser.add_argument('--month', type=int, required=True, help='Month to scrape')
    parser.add_argument('--day', type=int, required=True, help='Day to scrape')
    parser.add_argument('--force-scrape', action='store_true', help='Force scrape even if already scraped')
    
    args = parser.parse_args()
    
    scrape_single_day(args.year, args.month, args.day, args.force_scrape)

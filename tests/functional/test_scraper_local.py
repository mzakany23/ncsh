#!/usr/bin/env python3
"""
Pytest compatible test script for running the NC Soccer scraper locally.

This test script allows for testing the scraper functionality without
deploying to AWS, by running the scraper directly on the local machine.

Usage with pytest:
    pytest -xvs tests/functional/test_scraper_local.py

Or as a standalone script:
    python -m tests.functional.test_scraper_local --year 2025 --month 3 --day 14

Simple standalone script to test the scraper functionality locally.
This script directly calls the schedule spider and saves results locally.
"""
import os
import sys
import json
import time
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
    import scrapy
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings
    from ncsoccer.spiders.schedule_spider import ScheduleSpider
    from ncsoccer.pipeline.html_to_json import HTMLParser
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error("Make sure you've installed all required dependencies (pip install -r requirements.txt)")
    logger.error("And that you're running this script from the project root directory")
    sys.exit(1)

# Create a test spider class that forces scraping
class TestScheduleSpider(ScheduleSpider):
    """A test version of the ScheduleSpider that always forces scraping."""
    
    def is_date_scraped(self, year, month, day):
        """Override to always return False for testing purposes."""
        logger.info(f"Forcing scrape for test date: {year}-{month}-{day}")
        return False
        
    def _is_date_scraped(self, date):
        """Override internal date check method to always return False."""
        logger.info(f"Forcing scrape for date: {date}")
        return False
    
    def start_requests(self):
        """Override to ensure we're not skipping any dates."""
        logger.info(f"Using TestScheduleSpider which forces scraping for date: {self.target_year}-{self.target_month}-{self.target_day}")
        
        # Create the date string in the format expected by the schedule URL
        date_str = f"{self.target_year}-{self.target_month:02d}-{self.target_day:02d}"
        
        # Generate the URL directly using the spider's base URL
        url = self.start_urls[0]
        
        # Create a request and set the date in meta
        req = scrapy.Request(url=url, callback=self.parse_schedule)
        req.meta['date'] = date_str
        logger.info(f"Making direct request to {url} for date {date_str}")
        
        yield req

def run_test_scraper(year=None, month=None, day=None, output_dir=None, debug=False):
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
    if output_dir is None:
        # Use the test output directory by default
        project_root = Path(__file__).parent.parent.parent
        output_dir = project_root / 'tests' / 'output'
    
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
    
    # Create data directories for our HTML and JSON outputs if they don't exist
    os.makedirs(str(html_dir), exist_ok=True)
    os.makedirs(str(json_dir), exist_ok=True)
    
    # Debug info
    logger.info(f"HTML output directory: {html_dir}")
    logger.info(f"JSON output directory: {json_dir}")
    logger.info(f"HTML output file: {html_file}")
    logger.info(f"JSON output file: {json_file}")
    
    # Custom parser for HTML to JSON conversion
    html_parser = HTMLParser(year=year, month=month)
    
    # Create a dictionary to hold data between steps
    result_data = {'html': None, 'json': None}
    
    # Create settings and custom pipelines
    settings = get_project_settings()
    settings.set('LOG_LEVEL', 'INFO')
    
    # Function to save the spider output (injected into spider's context)
    def save_output(data, spider_instance):
        # Save HTML data
        result_data['html'] = data
        html_path = str(html_file)
        json_path = str(json_file)
        
        try:
            with open(html_path, 'w') as f:
                f.write(data)
            logger.info(f"Saved HTML data to {html_path}")
            
            # Process HTML to JSON
            json_data = html_parser.parse_daily_schedule(data, f"{year}-{month:02d}-{day:02d}")
            result_data['json'] = json_data
            
            # Save JSON data
            with open(json_path, 'w') as f:
                json.dump(json_data, f, indent=2)
            logger.info(f"Saved JSON data to {json_path}")
            
            return True
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
            return False
        
        # Save metadata
        meta_file = json_dir / f"{date_str}_meta.json"
        meta_data = {
            'scrape_time': datetime.now().isoformat(),
            'source': 'local_test',
            'date': date_str,
            'game_count': len(json_data)
        }
        with open(meta_file, 'w') as f:
            json.dump(meta_data, f, indent=2)
        
        logger.info(f"Scraped {len(json_data)} games for {date_str}")
        return True
    
    # Create crawler process with custom settings
    process = CrawlerProcess(settings)
    
    try:
        # Run our test spider that forces scraping
        process.crawl(
            TestScheduleSpider,
            year=year,
            month=month,
            day=day,
            output_file=str(html_file),
            storage_type='file',
            process_data=save_output,
            skip_existing=False  # For good measure, though our test spider ignores this
        )
        process.start()  # This will block until the crawl is finished
        
        # Give the system a moment to finish writing files
        time.sleep(1)
        
        # Check if files were created
        html_exists = os.path.exists(str(html_file))
        json_exists = os.path.exists(str(json_file))
        success = json_exists and html_exists
        
        if success:
            logger.info(f"Scrape successful! Output saved to {json_file}")
            # Check file contents to make sure they're valid
            with open(str(json_file), 'r') as f:
                json_data = json.load(f)
                logger.info(f"Successfully loaded JSON with {len(json_data)} records")
            return True
        else:
            logger.error(f"Scrape failed: HTML file exists: {html_exists}, JSON file exists: {json_exists}")
            logger.error(f"HTML path: {html_file}")
            logger.error(f"JSON path: {json_file}")
            return False
    
    except Exception as e:
        logger.error(f"Error running scraper: {str(e)}")
        return False

# Add pytest test function for easy integration with pytest
def test_scraper(ensure_test_dirs):
    """Run the scraper for a specific test date and verify that data is scraped correctly."""
    # Use the test output directory
    output_dir = ensure_test_dirs
    
    # Use a specific test date - pick a date that's unlikely to be skipped
    # Using January 15, 2025 as our test date
    test_year = 2025
    test_month = 1
    test_day = 15
    date_str = f"{test_year}-{test_month:02d}-{test_day:02d}"
    
    # Run the scraper and verify it was successful
    success = run_test_scraper(
        year=test_year,
        month=test_month,
        day=test_day,
        output_dir=output_dir,
        debug=True  # Enable debug mode for more verbose output
    )
    
    assert success, "Scraper test failed"
    
    # Now validate the content of the output files
    html_file = output_dir / 'html' / f"{date_str}.html"
    json_file = output_dir / 'json' / f"{date_str}.json"
    
    # Verify files exist
    assert os.path.exists(html_file), f"HTML file not created at {html_file}"
    assert os.path.exists(json_file), f"JSON file not created at {json_file}"
    
    # Verify HTML content contains expected elements
    with open(html_file, 'r') as f:
        html_content = f.read()
        # The test fixture has a different structure, check for expected elements
        assert "<html>" in html_content, "HTML doesn't contain valid markup"
        assert "<div class=\"game\">" in html_content, "HTML doesn't contain game information"
        assert "<div class=\"home-team\">" in html_content, "HTML doesn't contain home team information"
        assert "<div class=\"away-team\">" in html_content, "HTML doesn't contain away team information"
    
    # Verify JSON content structure and data
    with open(json_file, 'r') as f:
        json_data = json.load(f)
        
        # Verify we have data
        assert len(json_data) > 0, "No games were found in the JSON output"
        
        # Check the structure of a game entry
        game = json_data[0]
        required_fields = ['league', 'home_team', 'away_team', 'status', 'venue']
        for field in required_fields:
            assert field in game, f"Missing required field '{field}' in game data"
        
        # Verify the score fields if game is complete
        if game.get('status', '').lower() == 'complete':
            assert 'home_score' in game, "Missing home_score in completed game"
            assert 'away_score' in game, "Missing away_score in completed game"
            assert isinstance(game['home_score'], (int, float)), "Home score is not a number"
            assert isinstance(game['away_score'], (int, float)), "Away score is not a number"
    
    logger.info(f"Successfully validated scraped data for {date_str}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test NC Soccer scraper locally")
    parser.add_argument("--year", type=int, default=datetime.now().year,
                        help="Year to scrape (default: current year)")
    parser.add_argument("--month", type=int, default=datetime.now().month,
                        help="Month to scrape (default: current month)")
    parser.add_argument("--day", type=int, default=datetime.now().day,
                        help="Day to scrape (default: current day)")
    parser.add_argument("--output", type=str, default=None,
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

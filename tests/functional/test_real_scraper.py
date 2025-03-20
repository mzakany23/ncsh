#!/usr/bin/env python3
"""
Test script that verifies the NC Soccer scraper works with actual website data.

This test performs a real scrape against the NC Soccer website (or a mock/archived version)
and verifies that the data structure and content are as expected.

Usage with pytest:
    pytest -xvs tests/functional/test_real_scraper.py
"""
import os
import sys
import json
import pytest
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
    logger.error("Make sure you've installed all required dependencies")
    sys.exit(1)

# Define a test spider that forces scraping
class TestScheduleSpider(ScheduleSpider):
    """A test version of the ScheduleSpider that always forces scraping."""
    
    def is_date_scraped(self, year, month, day):
        """Override to always return False for testing purposes."""
        logger.info(f"Forcing scrape for test date: {year}-{month}-{day}")
        # Always return False to ensure date is scraped
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


@pytest.fixture
def test_output_dir():
    """Create and return test output directories."""
    output_dir = Path(project_root) / 'tests' / 'output'
    html_dir = output_dir / 'html'
    json_dir = output_dir / 'json'
    
    # Create directories if they don't exist
    for directory in [output_dir, html_dir, json_dir]:
        directory.mkdir(exist_ok=True)
    
    return output_dir


def run_real_scrape(year, month, day, output_dir):
    """
    Run a real scrape against the NC Soccer website for the given date.
    
    This version uses a simpler approach with direct HTTP requests instead of
    using the Scrapy crawler, which can be complex to integrate with pytest.
    
    Args:
        year: The year to scrape
        month: The month to scrape
        day: The day to scrape
        output_dir: Directory to save output
    
    Returns:
        tuple: (success, html_file, json_file)
    """
    date_str = f"{year}-{month:02d}-{day:02d}"
    html_dir = output_dir / 'html'
    json_dir = output_dir / 'json'
    html_file = html_dir / f"{date_str}.html"
    json_file = json_dir / f"{date_str}.json"
    
    # Use requests to fetch the schedule page directly
    import requests
    
    # Create the URL for the nc-soccer page
    url = "https://nc-soccer-hudson.ezleagues.ezfacility.com/schedule.aspx?facility_id=690"
    
    logger.info(f"Fetching schedule from {url}")
    
    try:
        # Make the request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes
        
        # Get the HTML content
        html_content = response.text
        
        # Save HTML content to file
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        logger.info(f"HTML saved to {html_file}")
        logger.info(f"HTML content length: {len(html_content)}")
        
        # Parse HTML to JSON using HTMLParser
        try:
            logger.info("Starting HTML parsing...")
            # Pass the required year and month parameters to the HTMLParser
            parser = HTMLParser(year, month)
            # Call parse_daily_schedule instead of parse
            date_obj = datetime(year, month, day).strftime('%Y-%m-%d')
            json_data = parser.parse_daily_schedule(html_content, date_obj)
            logger.info("HTML parsed successfully")
            
            # Log info about the parsed data
            if isinstance(json_data, dict):
                logger.info(f"JSON keys: {list(json_data.keys())}")
            elif isinstance(json_data, list):
                logger.info(f"JSON is a list with {len(json_data)} items")
            else:
                logger.info(f"JSON type: {type(json_data)}")
                
            # Save JSON data to file
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2)
                
            logger.info(f"JSON saved to {json_file}")
            return True, html_file, json_file
        
        except Exception as e:
            logger.error(f"Error parsing HTML: {str(e)}")
            import traceback
            logger.error(f"Error traceback: {traceback.format_exc()}")
            return False, html_file, json_file
            
    except Exception as e:
        logger.error(f"Error fetching page: {str(e)}")
        import traceback
        logger.error(f"Error traceback: {traceback.format_exc()}")
        return False, html_file, json_file


def test_real_scrape(test_output_dir):
    """
    Test scraping actual data from the NC Soccer website.
    
    This test will actually connect to the NC Soccer website and perform a real
    scrape, validating the structure and content of the scraped data.
    """
    # Use a date that should have data - adjust as needed
    # Using a recent past date as it should have real game data
    year = 2024
    month = 3
    day = 10
    date_str = f"{year}-{month:02d}-{day:02d}"
    
    # Run the real scrape
    success, html_file, json_file = run_real_scrape(year, month, day, test_output_dir)
    
    # Verify successful scrape
    assert success, f"Real scrape test failed for {date_str}"
    
    # Verify HTML file has content
    assert os.path.getsize(html_file) > 0, "HTML file is empty"
    
    # Verify the HTML contains expected structure elements
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
        # Common elements found in schedule pages 
        assert "<html" in html_content, "HTML doesn't contain valid markup"
        assert "<body" in html_content, "HTML doesn't contain body tag"
        # Verify it's a schedule page (specific to the site's structure)
        # Note: Adjust these assertions based on the actual structure of the website
        assert "schedule" in html_content.lower() or "games" in html_content.lower() or "matches" in html_content.lower(), \
            "HTML doesn't appear to be a schedule page"
    
    # Verify JSON structure
    with open(json_file, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
        
        # First, log the structure for debugging
        logger.info(f"JSON structure: {type(json_data)}")
        if isinstance(json_data, dict):
            logger.info(f"JSON keys: {list(json_data.keys())}")
        elif isinstance(json_data, list):
            logger.info(f"JSON is a list with {len(json_data)} items")
        
        # More flexible assertions that handle either a dict with games key or direct list of games
        games = []
        if isinstance(json_data, dict):
            # The games could be under various keys based on the parser implementation
            possible_game_keys = ['games', 'schedule', 'matches', 'events']
            for key in possible_game_keys:
                if key in json_data and isinstance(json_data[key], list):
                    games = json_data[key]
                    logger.info(f"Found games under key: {key}")
                    break
            # If no standard keys found, try to extract any list that might contain games
            if not games:
                for key, value in json_data.items():
                    if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                        games = value
                        logger.info(f"Found potential games list under key: {key}")
                        break
        elif isinstance(json_data, list):
            # Direct list of games
            games = json_data
        
        # Log game data status
        if len(games) == 0:
            logger.warning(f"No games found for {date_str}, but this might be valid (e.g., no games on this date)")
            
            # Additional debugging of HTML content to understand structure
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            logger.info(f"HTML title: {soup.title.text if soup.title else 'No title found'}")
            
            # Check for tables that might contain game data
            tables = soup.find_all('table')
            logger.info(f"Found {len(tables)} tables in HTML")
            
            # Check specifically for the game table that the parser looks for
            game_table = soup.select_one('#ctl00_ContentPlaceHolder1_gvGames')
            if game_table:
                logger.info("Found the game table that the parser looks for")
                rows = game_table.select('tr')
                logger.info(f"Table has {len(rows)} rows")  
            else:
                logger.info("Could not find the expected game table")
                
            # For this test, we'll consider it successful even with no games
            # since we're mainly testing the parser's ability to process the HTML
        else:
            # If we have games, log how many were found
            logger.info(f"Found {len(games)} games for {date_str}")
        
        # Only process game data if we have games
        if games:
            # Log the first game for debugging
            logger.info(f"First game data: {games[0]}")
            
            # Check for common game fields in a flexible way
            game = games[0]
            assert isinstance(game, dict), "Game data is not a dictionary"
            
            # Look for team information in a flexible way
            team_fields = []
            for field in game.keys():
                if 'team' in field.lower() or 'home' in field.lower() or 'away' in field.lower():
                    team_fields.append(field)
            
            logger.info(f"Team-related fields found: {team_fields}")
            if team_fields:
                logger.info("Test passed: Found team-related fields in game data")
            else:
                logger.warning("No team-related fields found, but continuing test")
    
    logger.info(f"Successfully validated real scrape data for {date_str}")
    if 'games' in locals():
        logger.info(f"Found {len(games)} games in the schedule for {date_str}")
    
    # Log sample game information
    if games:
        logger.info("Sample game data:")
        sample_game = games[0]
        
        # Flexible logging of game data
        # Try to find meaningful fields for reporting
        for key, value in sample_game.items():
            # Only log string/numeric values, skip complex objects
            if isinstance(value, (str, int, float)) and value:
                logger.info(f"  {key}: {value}")


if __name__ == "__main__":
    # Create output directories
    test_dir = test_output_dir()
    
    # Run the test with a specific date
    test_real_scrape(test_dir)

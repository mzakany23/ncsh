#!/usr/bin/env python3
"""Simple soccer schedule scraper using requests and BeautifulSoup."""

import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
from typing import Dict, List, Optional, Any, Union
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import traceback

# Import from pipeline modules
from ncsoccer.pipeline.config import (
    ScraperConfig,
    ScrapeMode,
    DataArchitectureVersion,
    StorageType,
    get_storage_interface,
    DataPathManager,
    create_scraper_config
)
from ncsoccer.pipeline.lookup import get_lookup_interface
from ncsoccer.pipeline.checkpoint import get_checkpoint_manager

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://nc-soccer-hudson.ezleagues.ezfacility.com"
PRINT_URL = f"{BASE_URL}/print.aspx"
FACILITY_ID = "690"

class SimpleScraper:
    """Simple soccer schedule scraper that replaces Scrapy implementation."""

    def __init__(
        self,
        mode: str = 'day',
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        start_year: Optional[int] = None,
        start_month: Optional[int] = None,
        start_day: Optional[int] = None,
        end_year: Optional[int] = None,
        end_month: Optional[int] = None,
        end_day: Optional[int] = None,
        skip_existing: bool = True,
        html_prefix: str = 'data/html',
        json_prefix: str = 'data/json',
        lookup_file: str = 'data/lookup.json',
        storage_type: str = 's3',
        bucket_name: Optional[str] = None,
        lookup_type: str = 'file',
        region: str = 'us-east-2',
        table_name: Optional[str] = None,
        force_scrape: bool = False,
        use_test_data: bool = False,
        architecture_version: str = 'v1',
        max_workers: int = 4,
        timeout: int = 30,
        max_retries: int = 3,
        session: Optional[requests.Session] = None
    ):
        """Initialize the scraper.

        Args:
            mode: 'day' for single day mode or 'range' for date range mode
            year: Year to scrape for single day mode
            month: Month to scrape for single day mode
            day: Day to scrape for single day mode
            start_year: Start year for date range mode
            start_month: Start month for date range mode
            start_day: Start day for date range mode
            end_year: End year for date range mode
            end_month: End month for date range mode
            end_day: End day for date range mode
            skip_existing: Whether to skip dates that have already been scraped
            html_prefix: Prefix for HTML files
            json_prefix: Prefix for JSON files
            lookup_file: Path to lookup file
            storage_type: 'file' or 's3'
            bucket_name: S3 bucket name if storage_type is 's3'
            lookup_type: Only 'file' is supported
            region: AWS region
            table_name: Deprecated, will be ignored
            force_scrape: Whether to scrape even if date already exists
            use_test_data: Whether to use test data paths
            architecture_version: 'v1' for legacy or 'v2' for new data architecture
            max_workers: Maximum number of concurrent workers for parallel scraping
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts for failed requests
            session: Optional requests.Session to use for all requests
        """
        # Set mode ('day' or 'range')
        self.scrape_mode = mode

        # Current date for defaults
        now = datetime.now()

        # Force scrape flag (override skip_existing)
        self.force_scrape = force_scrape

        # Whether to skip already scraped dates
        self.skip_existing = skip_existing and not force_scrape

        # Parse parameters for single date mode
        self.target_year = int(year) if year else now.year
        self.target_month = int(month) if month else now.month
        self.target_day = int(day) if day else now.day

        # Parse date range configuration
        self.start_year = int(start_year) if start_year else 2007  # Default to 2007
        self.start_month = int(start_month) if start_month else 1   # Default to January
        self.start_day = int(start_day) if start_day else 1         # Default to first day

        self.end_year = int(end_year) if end_year else now.year
        self.end_month = int(end_month) if end_month else now.month
        self.end_day = int(end_day) if end_day else None  # None means last day of month

        # Statistics
        self.start_time = time.time()
        self.games_scraped = 0

        # Architecture version
        self.architecture_version = architecture_version

        # HTTP settings
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.max_workers = max_workers

        # Setup user agent and other headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        })

        # Detect Lambda environment - if we're in Lambda, adjust paths
        self.in_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
        if self.in_lambda:
            logger.info("Running in Lambda environment - adjusting paths")
            # Ensure S3 storage for Lambda
            if storage_type != 's3':
                logger.warning("Forcing S3 storage for Lambda environment")
                storage_type = 's3'

            # Get bucket name from environment if not provided
            if not bucket_name:
                bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')

            # For v2 architecture, we should always use S3 in Lambda
            if architecture_version == 'v2' and lookup_type != 's3':
                logger.warning("Forcing S3 lookup for v2 architecture in Lambda")
                lookup_type = 's3'

            # Adjust file paths for /tmp if needed, but only if not using v2
            if architecture_version != 'v2':
                if html_prefix.startswith('data/'):
                    html_prefix = f"/tmp/{html_prefix}"
                    logger.info(f"Adjusted html_prefix for Lambda: {html_prefix}")

                if json_prefix.startswith('data/'):
                    json_prefix = f"/tmp/{json_prefix}"
                    logger.info(f"Adjusted json_prefix for Lambda: {json_prefix}")

                if lookup_file.startswith('data/'):
                    lookup_file = f"/tmp/{lookup_file}"
                    logger.info(f"Adjusted lookup_file for Lambda: {lookup_file}")

        # Set up path manager for data architecture
        self.path_manager = DataPathManager(
            architecture_version=architecture_version,
            base_prefix=html_prefix if html_prefix and not html_prefix.endswith('/html') else ('test_data' if use_test_data else '')
        )

        # For backward compatibility (these are used in various parts of the code)
        if use_test_data:
            self.html_prefix = 'test_data/html'
            self.json_prefix = 'test_data/json'
        else:
            self.html_prefix = html_prefix
            self.json_prefix = json_prefix
        self.lookup_file = lookup_file

        # Create scraper configuration
        self.config = create_scraper_config(
            mode='day',  # Always use day mode, we'll handle multiple days externally
            year=self.target_year,
            month=self.target_month,
            day=self.target_day,
            skip_existing=self.skip_existing,
            storage_type=storage_type,
            bucket_name=bucket_name,
            architecture_version=architecture_version
        )

        # Set up storage and lookup interfaces
        self.storage = get_storage_interface(self.config.storage_type, self.config.bucket_name, region=region)
        self.lookup = get_lookup_interface(
            lookup_type=lookup_type,
            lookup_file=lookup_file,
            region=region,
            table_name=table_name,
            architecture_version=architecture_version
        )

        # Set up checkpoint manager if using v2 architecture
        self.checkpoint = None
        if architecture_version == 'v2':
            checkpoint_path = self.path_manager.get_checkpoint_path()
            self.checkpoint = get_checkpoint_manager(checkpoint_path, storage_interface=self.storage)
            logger.info(f"Checkpoint manager initialized for {checkpoint_path}")

        # Log scrape configuration
        if self.scrape_mode == 'range':
            logger.info(f"Date range scrape: {self.start_year}-{self.start_month:02d}-{self.start_day:02d} to {self.end_year}-{self.end_month:02d}-{self.end_day or 'last day'}")
        else:
            logger.info(f"Single date scrape: {self.target_year}-{self.target_month:02d}-{self.target_day:02d}")

    def date_already_scraped(self, date_obj: datetime) -> bool:
        """Check if a date has already been scraped.

        Args:
            date_obj: datetime object for the date to check

        Returns:
            Whether the date has already been scraped
        """
        date_str = date_obj.strftime('%Y-%m-%d')

        # Always scrape if force_scrape is True
        if self.force_scrape:
            logger.info(f"Force scrape enabled, ignoring previous scrape status for {date_str}")
            return False

        # Check checkpoint if using v2 architecture
        if self.checkpoint:
            is_scraped = self.checkpoint.is_date_scraped(date_str)
            if is_scraped:
                logger.info(f"Date {date_str} already scraped according to checkpoint")
            return is_scraped

        # Otherwise check lookup
        is_scraped = self.lookup.is_date_scraped(date_str)
        if is_scraped:
            logger.info(f"Date {date_str} already scraped according to lookup")
        return is_scraped

    def get_direct_date_url(self, date_obj: datetime) -> str:
        """Generate direct URL for a specific date.

        Args:
            date_obj: datetime object for the date

        Returns:
            URL for the print.aspx page with appropriate query parameters
        """
        # Format date for display in title (e.g., "Sunday, March 23, 2025")
        formatted_date = date_obj.strftime('%A, %B %d, %Y')

        params = {
            'type': 'schedule',
            'title': f'Games on {formatted_date}',
            'team_id': '0',
            'league_id': '0',
            'facility_id': FACILITY_ID,
            'day': date_obj.strftime('%m/%d/%Y'),
            'framed': '1'
        }
        return f"{PRINT_URL}?{urllib.parse.urlencode(params)}"

    def fetch_schedule_page(self, date_obj: datetime) -> Optional[str]:
        """Fetch the schedule page HTML for a specific date.

        Args:
            date_obj: datetime object for the date

        Returns:
            HTML content of the page, or None if the request failed
        """
        url = self.get_direct_date_url(date_obj)
        logger.info(f"Fetching schedule page for {date_obj.strftime('%Y-%m-%d')} from {url}")

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code == 200:
                    logger.info(f"Successfully fetched schedule page for {date_obj.strftime('%Y-%m-%d')}")
                    return response.text
                else:
                    logger.warning(f"Failed to fetch schedule page (status {response.status_code}), attempt {attempt + 1}/{self.max_retries}")
            except requests.RequestException as e:
                logger.warning(f"Request error: {e}, attempt {attempt + 1}/{self.max_retries}")

            # Wait before retrying (increasing backoff)
            if attempt < self.max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff

        logger.error(f"Failed to fetch schedule page after {self.max_retries} attempts")
        return None

    def save_html(self, date_obj: datetime, html_content: str) -> bool:
        """Save HTML content to storage.

        Args:
            date_obj: datetime object for the date
            html_content: HTML content to save

        Returns:
            Whether the save was successful
        """
        html_path = self.path_manager.get_html_path(date_obj)
        logger.info(f"Saving HTML to {html_path}")

        try:
            self.storage.write(html_path, html_content)
            logger.info(f"Successfully saved HTML to {html_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving HTML: {e}")
            return False

    def parse_schedule_page(self, html_content: str, date_obj: datetime) -> List[Dict[str, Any]]:
        """Parse the schedule page HTML to extract games.

        Args:
            html_content: HTML content to parse
            date_obj: datetime object for the date (used for metadata)

        Returns:
            List of game dictionaries
        """
        logger.info(f"Parsing schedule page for {date_obj.strftime('%Y-%m-%d')}")
        soup = BeautifulSoup(html_content, 'html.parser')

        games = []
        date_str = date_obj.strftime('%Y-%m-%d')

        # Find the schedule table - try different possible IDs based on the page format
        schedule_table = soup.find('table', id='ctl00_c_Schedule1_GridView1')
        if not schedule_table:
            schedule_table = soup.find('table', id='ctl04_GridView1')

        if not schedule_table:
            # Try the old format as fallback with class
            schedule_table = soup.find('table', class_='table-striped')

        if not schedule_table:
            # Try any table with the class containing "ezl-base-table"
            schedule_table = soup.find('table', class_=lambda c: c and 'ezl-base-table' in c)

        if not schedule_table:
            logger.warning(f"No schedule table found for {date_str}")
            return games

        # Extract game rows - skip first row (header)
        rows = schedule_table.find_all('tr')[1:] if schedule_table.find_all('tr') else []

        for row in rows:
            # Skip rows without enough cells
            cells = row.find_all('td')
            if len(cells) < 5:  # Need at least 5 cells for minimum data
                continue

            try:
                # First detect the table structure by examining header or data-th attributes
                table_format = "unknown"

                # Check for data-th attributes which indicate the modern format
                if 'data-th' in str(cells[0]):
                    # Modern format with data-th attributes
                    table_format = "modern"

                    # Extract based on data-th values
                    league_name = ""
                    home_team = ""
                    away_team = ""
                    status = ""
                    venue = ""
                    officials = ""
                    score = ""

                    for cell in cells:
                        data_th = cell.get('data-th', '').strip().lower()
                        cell_text = cell.get_text(strip=True)

                        if data_th == 'league':
                            league_name = cell_text
                        elif data_th == 'home':
                            home_team = cell_text
                        elif data_th == 'away':
                            away_team = cell_text
                        elif data_th == 'time/status':
                            status = cell_text
                        elif data_th == 'venue':
                            venue = cell_text
                        elif data_th == 'officials':
                            officials = cell_text
                        elif data_th == '':  # Check for score in versus column
                            if ' - ' in cell_text and cell_text.replace(' - ', '').strip().isdigit():
                                score = cell_text

                else:
                    # Legacy format - determine by column count and content
                    table_format = "legacy"

                    # The layout appears to be different, with "Sat-Feb 15" often in the "home_team" position
                    # and scores in the format "3 - 2" often in the "away_team" position

                    league_name = cells[0].get_text(strip=True) if len(cells) > 0 else ""

                    # In the legacy format, the next cell often contains the team or game info
                    game_info = cells[1].get_text(strip=True) if len(cells) > 1 else ""

                    # Check for date format (e.g., "Sat-Feb 15") which indicates this is the column layout
                    date_indicator = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    if date_indicator and "Sat-" in date_indicator or "Sun-" in date_indicator:
                        home_team = game_info  # Team name is in the previous cell

                        # Score or versus indicator is usually in the next cell
                        score_or_vs = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                        if " - " in score_or_vs and any(c.isdigit() for c in score_or_vs):
                            score = score_or_vs
                            away_team = ""  # We don't have a clear away team in this format
                        else:
                            score = ""
                            away_team = score_or_vs

                        # Status or field is in the next cell
                        status = cells[4].get_text(strip=True) if len(cells) > 4 else ""

                        # Venue or field is usually after status
                        venue = cells[5].get_text(strip=True) if len(cells) > 5 else ""

                        # Officials might be in the last column
                        officials = cells[6].get_text(strip=True) if len(cells) > 6 else ""
                    else:
                        # Different column layout where team names are in separate columns
                        home_team = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                        away_team = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                        status = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                        venue = cells[4].get_text(strip=True) if len(cells) > 4 else ""
                        officials = cells[5].get_text(strip=True) if len(cells) > 5 else ""
                        score = ""

                # Create game item
                game = {
                    'league_name': league_name,
                    'game_date': date_str,
                    'game_time': status,  # Using status as game_time
                    'home_team': home_team,
                    'away_team': away_team,
                    'score': score,
                    'field': venue,
                    'game_type': "",  # We don't have consistent game type data
                    'officials': officials,
                    'facility_id': FACILITY_ID,
                    '_table_format': table_format  # Store the detected format for debugging
                }

                games.append(game)
                self.games_scraped += 1

            except Exception as e:
                logger.error(f"Error parsing game row: {e}")
                logger.error(f"Row content: {row}")

        logger.info(f"Extracted {len(games)} games for {date_str}")
        return games

    def save_json(self, date_obj: datetime, games: List[Dict[str, Any]]) -> bool:
        """Save games data to JSON.

        Args:
            date_obj: datetime object for the date
            games: List of game dictionaries

        Returns:
            Whether the save was successful
        """
        try:
            # Save meta data
            meta_path = self.path_manager.get_json_meta_path(date_obj)
            meta_data = {
                'date': date_obj.strftime('%Y-%m-%d'),
                'games_count': len(games),
                'scraped_timestamp': datetime.now().isoformat()
            }
            logger.info(f"Saving metadata to {meta_path}")
            self.storage.write(meta_path, json.dumps(meta_data, indent=2))

            # Save games data
            games_path = self.path_manager.get_games_path(date_obj)
            logger.info(f"Saving games data to {games_path}")
            self.storage.write(games_path, json.dumps(games, indent=2))

            logger.info(f"Successfully saved JSON data for {date_obj.strftime('%Y-%m-%d')}")
            return True
        except Exception as e:
            logger.error(f"Error saving JSON data: {e}")
            return False

    def update_checkpoint(self, date_obj: datetime, success: bool, games_count: int) -> bool:
        """Update checkpoint or lookup data.

        Args:
            date_obj: datetime object for the date
            success: Whether scraping was successful
            games_count: Number of games scraped

        Returns:
            Whether the update was successful
        """
        date_str = date_obj.strftime('%Y-%m-%d')

        try:
            if self.checkpoint:
                # Use checkpoint manager for v2 architecture
                # Always mark as completed if we successfully processed the page, even if 0 games
                self.checkpoint.update_scraping(date_str, success=success, games_count=games_count)
                # Verify the checkpoint was updated
                checkpoint_data = self.checkpoint.get_checkpoint_data()
                if date_str not in checkpoint_data.get('completed_dates', []):
                    logger.warning(f"Checkpoint for {date_str} was not properly updated. Attempting again.")
                    # Try one more time
                    self.checkpoint.update_scraping(date_str, success=success, games_count=games_count, force=True)
                    checkpoint_data = self.checkpoint.get_checkpoint_data()
                    if date_str not in checkpoint_data.get('completed_dates', []):
                        logger.error(f"Failed to update checkpoint for {date_str} after retry.")
                        return False
                logger.info(f"Updated checkpoint for {date_str} with games_count={games_count}")
            else:
                # Use lookup for v1 architecture
                self.lookup.update_date(date_str, success=success, games_count=games_count)
                logger.info(f"Updated lookup for {date_str} with games_count={games_count}")
            return True
        except Exception as e:
            logger.error(f"Error updating checkpoint: {e}")
            traceback.print_exc()
            return False

    def scrape_date(self, date_obj: datetime) -> bool:
        """Scrape data for a specific date.

        Args:
            date_obj: datetime object for the date

        Returns:
            Whether scraping was successful
        """
        date_str = date_obj.strftime('%Y-%m-%d')
        logger.info(f"Scraping date: {date_str}")

        # Check if date already scraped
        if self.skip_existing and self.date_already_scraped(date_obj):
            logger.info(f"Skipping already scraped date: {date_str}")
            return True

        # Fetch page
        html_content = self.fetch_schedule_page(date_obj)
        if not html_content:
            logger.error(f"Failed to fetch page for {date_str}")
            return False

        # Save HTML
        if not self.save_html(date_obj, html_content):
            logger.error(f"Failed to save HTML for {date_str}")
            return False

        # Parse HTML
        games = self.parse_schedule_page(html_content, date_obj)

        # Save JSON
        if not self.save_json(date_obj, games):
            logger.error(f"Failed to save JSON for {date_str}")
            return False

        # Update checkpoint
        if not self.update_checkpoint(date_obj, True, len(games)):
            logger.warning(f"Failed to update checkpoint for {date_str}")
            # Non-critical error, continue

        logger.info(f"Successfully scraped {len(games)} games for {date_str}")
        return True

    def scrape_date_range(self, start_date: datetime, end_date: datetime, parallel: bool = True) -> Dict[str, bool]:
        """Scrape data for a range of dates.

        Args:
            start_date: Start date
            end_date: End date
            parallel: Whether to scrape dates in parallel

        Returns:
            Dictionary mapping dates to success status
        """
        logger.info(f"Scraping date range from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        # Generate list of dates to scrape
        current_date = start_date
        dates = []
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)

        logger.info(f"Found {len(dates)} dates to scrape")

        results = {}

        if parallel and len(dates) > 1:
            # Use ThreadPoolExecutor for parallel scraping
            with ThreadPoolExecutor(max_workers=min(self.max_workers, len(dates))) as executor:
                # Submit all scraping tasks
                future_to_date = {
                    executor.submit(self.scrape_date, date): date for date in dates
                }

                # Collect results as they complete
                for future in future_to_date:
                    date = future_to_date[future]
                    date_str = date.strftime('%Y-%m-%d')
                    try:
                        success = future.result()
                        results[date_str] = success
                    except Exception as e:
                        logger.error(f"Error scraping {date_str}: {e}")
                        results[date_str] = False
        else:
            # Scrape dates sequentially
            for date in dates:
                date_str = date.strftime('%Y-%m-%d')
                try:
                    success = self.scrape_date(date)
                    results[date_str] = success
                except Exception as e:
                    logger.error(f"Error scraping {date_str}: {e}")
                    results[date_str] = False

        # Calculate success ratio
        success_count = sum(1 for success in results.values() if success)
        logger.info(f"Scraped {success_count}/{len(dates)} dates successfully")

        return results

    def run(self) -> bool:
        """Run the scraper according to configured mode.

        Returns:
            Whether scraping was successful overall
        """
        start_time = time.time()

        try:
            if self.scrape_mode == 'range':
                # Determine start and end dates
                start_date = datetime(self.start_year, self.start_month, self.start_day)

                # If end_day is None, use the last day of the month
                if self.end_day is None:
                    if self.end_month == 12:
                        last_day = 31
                    else:
                        last_day = (datetime(self.end_year, self.end_month + 1, 1) - timedelta(days=1)).day
                    end_date = datetime(self.end_year, self.end_month, last_day)
                else:
                    end_date = datetime(self.end_year, self.end_month, self.end_day)

                # Scrape date range
                results = self.scrape_date_range(start_date, end_date)
                success = all(results.values())
            else:
                # Scrape single date
                target_date = datetime(self.target_year, self.target_month, self.target_day)
                success = self.scrape_date(target_date)

            end_time = time.time()
            duration = end_time - start_time

            logger.info(f"Scraping completed in {duration:.2f} seconds")
            logger.info(f"Scraped {self.games_scraped} games total")

            return success

        except Exception as e:
            logger.error(f"Error running scraper: {e}", exc_info=True)
            return False


def scrape_single_date(year, month, day, **kwargs):
    """Helper function to scrape a single date.

    Args:
        year: Year to scrape
        month: Month to scrape
        day: Day to scrape
        **kwargs: Additional arguments to pass to SimpleScraper

    Returns:
        Whether scraping was successful
    """
    scraper = SimpleScraper(
        mode='day',
        year=year,
        month=month,
        day=day,
        **kwargs
    )
    return scraper.run()


def scrape_date_range(start_date, end_date, **kwargs):
    """Helper function to scrape a date range.

    Args:
        start_date: Start date (datetime object)
        end_date: End date (datetime object)
        **kwargs: Additional arguments to pass to SimpleScraper

    Returns:
        Whether scraping was successful
    """
    scraper = SimpleScraper(
        mode='range',
        start_year=start_date.year,
        start_month=start_date.month,
        start_day=start_date.day,
        end_year=end_date.year,
        end_month=end_date.month,
        end_day=end_date.day,
        **kwargs
    )
    return scraper.run()
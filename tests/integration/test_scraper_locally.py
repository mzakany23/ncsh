#!/usr/bin/env python3
"""
Integration test for the NC Soccer scraper.
This allows testing of the scraper functionality without deploying to AWS.
"""
import os
import sys
import json
import logging
import unittest
from datetime import datetime, timedelta
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Now import the runner module
from scraping.ncsoccer.runner import run_scraper, run_month

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress DEBUG messages from third-party libraries
logging.getLogger("twisted").setLevel(logging.WARNING)
logging.getLogger("scrapy").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)

class TestScraperIntegration(unittest.TestCase):
    """Integration test class for the scraper module"""
    
    def setUp(self):
        """Set up test environment before each test method."""
        # Create test data directories
        self.test_dir = Path('./test_output')
        self.html_dir = self.test_dir / 'html'
        self.json_dir = self.test_dir / 'json'
        self.html_dir.mkdir(parents=True, exist_ok=True)
        self.json_dir.mkdir(parents=True, exist_ok=True)
        
        # Create an empty scraped_dates.json file for tracking
        self.lookup_file = self.test_dir / 'scraped_dates.json'
        if not self.lookup_file.exists():
            with open(self.lookup_file, 'w') as f:
                json.dump({}, f)
        
        # Common parameters for scraper
        self.params = {
            'storage_type': 'local',
            'bucket_name': None,  # Not used in local mode
            'html_prefix': 'html',
            'json_prefix': 'json',
            'lookup_type': 'file',
            'lookup_file': str(self.lookup_file),
            'force_scrape': True,
            'region': 'us-east-2',
            'use_test_data': False
        }
        
        # Default date (yesterday to ensure we have data)
        self.yesterday = datetime.now() - timedelta(days=1)
        self.year = self.yesterday.year
        self.month = self.yesterday.month
        self.day = self.yesterday.day
    
    def tearDown(self):
        """Clean up after each test method."""
        # We could delete test files here, but for debugging we'll leave them
        pass
    
    def test_day_scraper(self):
        """Test the day scraper can successfully scrape a day."""
        result = run_scraper(
            year=self.year,
            month=self.month,
            day=self.day,
            **self.params
        )
        
        # Verify the scraper returned a success status
        self.assertTrue(result.get('success', False))
        
        # Check that output files were created
        date_str = f"{self.year}-{self.month:02d}-{self.day:02d}"
        json_file = self.json_dir / f"{date_str}.json"
        meta_file = self.json_dir / f"{date_str}_meta.json"
        
        self.assertTrue(json_file.exists())
        self.assertTrue(meta_file.exists())
        
        # Verify json file contains expected data
        with open(json_file, 'r') as f:
            json_data = json.load(f)
            self.assertIsInstance(json_data, list)
            
        # Verify meta file contains expected data
        with open(meta_file, 'r') as f:
            meta_data = json.load(f)
            self.assertIsInstance(meta_data, dict)
            self.assertIn('scrape_time', meta_data)
    
    def test_month_scraper(self):
        """Test the month scraper can successfully scrape a month."""
        result = run_month(
            year=self.year,
            month=self.month,
            **self.params
        )
        
        # Verify the scraper returned a success status
        self.assertTrue(result.get('success', False))
        
        # Check that at least one output file was created
        month_str = f"{self.year}-{self.month:02d}"
        json_files = list(self.json_dir.glob(f"{month_str}-*.json"))
        
        # We should have at least one file (non-meta) for the month
        self.assertTrue(any(not f.name.endswith('_meta.json') for f in json_files))
        
        # Verify at least one json file has content
        found_valid_file = False
        for json_file in json_files:
            if json_file.name.endswith('_meta.json'):
                continue
                
            with open(json_file, 'r') as f:
                json_data = json.load(f)
                if len(json_data) > 0:
                    found_valid_file = True
                    break
                    
        self.assertTrue(found_valid_file)

def run_manual_test(mode, year, month, day=None, force=True):
    """Run a manual test of the scraper outside the unittest framework."""
    # Create test directories
    test_dir = Path('./test_output')
    html_dir = test_dir / 'html'
    json_dir = test_dir / 'json'
    html_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    
    # Create lookup file
    lookup_file = test_dir / 'scraped_dates.json'
    if not lookup_file.exists():
        with open(lookup_file, 'w') as f:
            json.dump({}, f)
    
    # Common parameters
    params = {
        'storage_type': 'local',
        'bucket_name': None,  # Not used in local mode
        'html_prefix': 'html',
        'json_prefix': 'json',
        'lookup_type': 'file',
        'lookup_file': str(lookup_file),
        'force_scrape': force,
        'region': 'us-east-2',
        'use_test_data': False
    }
    
    # Run appropriate scraper
    if mode == 'day':
        if day is None:
            day = datetime.now().day
        
        logger.info(f"Testing day scraper for {year}-{month:02d}-{day:02d}")
        result = run_scraper(
            year=year,
            month=month,
            day=day,
            **params
        )
    else:
        logger.info(f"Testing month scraper for {year}-{month:02d}")
        result = run_month(
            year=year,
            month=month,
            **params
        )
    
    logger.info(f"Scraper result: {result}")
    return result

if __name__ == "__main__":
    import argparse
    
    # Check if running tests or manual execution
    if len(sys.argv) > 1 and sys.argv[1] == '--unittest':
        # Run unit tests
        unittest.main(argv=['first-arg-is-ignored'])
    else:
        # Parse command line arguments for manual execution
        parser = argparse.ArgumentParser(description="Local testing for NC Soccer scraper")
        parser.add_argument("--mode", choices=["day", "month"], default="day", 
                            help="Scraping mode: day or month")
        parser.add_argument("--year", type=int, default=datetime.now().year,
                            help="Year to scrape (default: current year)")
        parser.add_argument("--month", type=int, default=datetime.now().month,
                            help="Month to scrape (default: current month)")
        parser.add_argument("--day", type=int, default=None,
                            help="Day to scrape (only for day mode, default: current day)")
        parser.add_argument("--force", action="store_true", default=True,
                            help="Force scrape even if already scraped")
        
        args = parser.parse_args()
        
        # Run the manual test
        run_manual_test(args.mode, args.year, args.month, args.day, args.force)

#!/usr/bin/env python3
"""
Direct Scraper Test

This module contains tests for the direct scraper functionality.
It provides a way to test the scraper without AWS dependencies.
"""

import os
import sys
import json
import logging
import calendar
import argparse
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('direct-scraper-test')

def setup_test_environment():
    """Set up the environment for testing"""
    # Determine the repository root
    repo_root = Path(__file__).parent.parent.parent.parent.absolute()
    
    # Add the scripts and scraping directories to the Python path
    scripts_path = repo_root / "scripts"
    scraping_path = repo_root / "scraping"
    sys.path.insert(0, str(scripts_path))
    sys.path.insert(0, str(scraping_path))
    
    # Create test output directories
    data_dir = repo_root / "tests" / "output" / "data"
    html_dir = data_dir / "html"
    json_dir = data_dir / "json"
    
    html_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    
    return repo_root, data_dir, html_dir, json_dir

def test_day_mode():
    """Test the direct scraper in day mode"""
    repo_root, data_dir, html_dir, json_dir = setup_test_environment()
    
    # Import the direct scraper module
    from direct_scraper import run_scrape
    
    # Test scraping a specific day
    year = 2025
    month = 3
    day = 14
    
    logger.info(f"Testing day mode for {year}-{month}-{day}")
    
    success_count, html_count, json_count = run_scrape(
        mode='day',
        year=year,
        month=month,
        day=day,
        force_scrape=True
    )
    
    assert success_count == 1, "Day mode scraping failed"
    assert html_count > 0, "No HTML files were generated"
    assert json_count > 0, "No JSON files were generated"
    
    logger.info(f"Day mode test passed: {html_count} HTML files, {json_count} JSON files")
    return True

def test_month_mode():
    """Test the direct scraper in month mode"""
    repo_root, data_dir, html_dir, json_dir = setup_test_environment()
    
    # Import the direct scraper module
    from direct_scraper import run_scrape
    
    # Test scraping a specific month
    year = 2025
    month = 3
    
    logger.info(f"Testing month mode for {year}-{month}")
    
    # Get the number of days in the month for assertions
    days_in_month = calendar.monthrange(year, month)[1]
    
    success_count, html_count, json_count = run_scrape(
        mode='month',
        year=year,
        month=month,
        force_scrape=True
    )
    
    assert success_count > 0, "Month mode scraping failed completely"
    assert html_count > 0, "No HTML files were generated"
    assert json_count > 0, "No JSON files were generated"
    
    logger.info(f"Month mode test passed: {success_count} days processed, {html_count} HTML files, {json_count} JSON files")
    return True

def run_tests():
    """Run all the tests"""
    results = {
        "day_mode": test_day_mode(),
        "month_mode": test_month_mode()
    }
    
    all_passed = all(results.values())
    
    if all_passed:
        logger.info("✅ All tests passed!")
    else:
        failed_tests = [name for name, passed in results.items() if not passed]
        logger.error(f"❌ Some tests failed: {', '.join(failed_tests)}")
    
    return all_passed

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test the direct scraper')
    parser.add_argument('--test', choices=['day', 'month', 'all'], default='all',
                        help='Which test to run')
    
    args = parser.parse_args()
    
    if args.test == 'day':
        test_day_mode()
    elif args.test == 'month':
        test_month_mode()
    else:
        run_tests()

#!/usr/bin/env python3
"""
Direct Scraper for NC Soccer

Main script for direct scraping of NC Soccer games data.
Supports both day and month modes.
This script can be used locally to test and validate the scraping process
without relying on AWS deployments.
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
logger = logging.getLogger('direct-scraper')

def setup_environment(repo_root=None):
    """Set up the environment for scraping"""
    # Determine the repository root
    if repo_root is None:
        repo_root = Path(__file__).parent.parent.absolute()
    
    # Add the scraping directory to the Python path
    scraping_path = repo_root / "scraping"
    sys.path.insert(0, str(scraping_path))
    
    # Create output directories
    data_dir = repo_root / "output" / "data"
    html_dir = data_dir / "html"
    json_dir = data_dir / "json"
    
    html_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    
    return repo_root, data_dir, html_dir, json_dir

def run_scrape(mode='day', year=None, month=None, day=None, force_scrape=False):
    """
    Run the direct scraper in either day or month mode
    
    Args:
        mode (str): 'day' or 'month'
        year (int): Year to scrape
        month (int): Month to scrape
        day (int): Day to scrape (only used in day mode)
        force_scrape (bool): Force scrape even if data already exists
        
    Returns:
        tuple: (success_count, html_count, json_count)
    """
    repo_root, data_dir, html_dir, json_dir = setup_environment()
    
    # Import scripts after setting up the path
    sys.path.insert(0, str(Path(__file__).parent))
    
    if mode == 'day':
        from scrape_single_day import scrape_single_day
        logger.info(f"Starting direct scraper in day mode for {year}-{month}-{day}")
        
        html_count, json_count = scrape_single_day(year, month, day, force_scrape)
        return 1 if (html_count > 0 or json_count > 0) else 0, html_count, json_count
    
    elif mode == 'month':
        from scrape_month import scrape_month
        logger.info(f"Starting direct scraper in month mode for {year}-{month}")
        
        return scrape_month(year, month, force_scrape)
    
    else:
        logger.error(f"Invalid mode: {mode}. Must be 'day' or 'month'")
        return 0, 0, 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Direct scraper for NC Soccer')
    parser.add_argument('--mode', type=str, default='day', choices=['day', 'month'], 
                        help='Scraping mode: day or month')
    parser.add_argument('--year', type=int, default=datetime.now().year, 
                        help='Year to scrape')
    parser.add_argument('--month', type=int, default=datetime.now().month, 
                        help='Month to scrape')
    parser.add_argument('--day', type=int, default=datetime.now().day,
                        help='Day to scrape (only used in day mode)')
    parser.add_argument('--force-scrape', action='store_true', 
                        help='Force scrape even if already scraped')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.mode == 'day' and args.day is None:
        parser.error("--day is required when mode is 'day'")
    
    # Run the scraper
    success_count, html_count, json_count = run_scrape(
        mode=args.mode,
        year=args.year,
        month=args.month,
        day=args.day,
        force_scrape=args.force_scrape
    )
    
    # Output results
    if success_count > 0:
        print(f"✅ Scraping completed successfully!")
        print(f"Output directory: {Path(__file__).parent.parent.absolute() / 'output' / 'data'}")
        print(f"Generated {html_count} HTML files and {json_count} JSON files")
        sys.exit(0)
    else:
        print(f"❌ Scraping failed. No data was successfully processed.")
        sys.exit(1)

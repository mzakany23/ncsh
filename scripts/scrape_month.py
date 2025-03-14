#!/usr/bin/env python3
"""
Month Scraper for NC Soccer

This script handles scraping a full month's worth of games data.
It runs parallel scrapers for each day of the month to improve efficiency.
"""

import os
import sys
import json
import logging
import calendar
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('month-scraper')

def setup_environment(repo_root=None):
    """Set up the environment for scraping"""
    # Determine the repository root
    if repo_root is None:
        repo_root = Path(__file__).parent.parent.absolute()
    
    # Create output directories
    data_dir = repo_root / "output" / "data"
    html_dir = data_dir / "html"
    json_dir = data_dir / "json"
    
    html_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    
    return repo_root, data_dir, html_dir, json_dir

def scrape_month(year, month, force_scrape=False):
    """Scrape all days in a month"""
    repo_root, data_dir, html_dir, json_dir = setup_environment()
    
    # Calculate number of days in the month
    days_in_month = calendar.monthrange(year, month)[1]
    
    logger.info(f"Starting month scrape for {year}-{month}, {days_in_month} days to process")
    
    # Define how many days to process in parallel (adjust based on your system)
    max_workers = min(multiprocessing.cpu_count(), 4)  # Use at most 4 cores
    
    # Create a list to store results
    successful_days = 0
    
    # Get the path to the helper script
    helper_script = os.path.join(repo_root, 'scripts', 'scrape_single_day.py')
    
    # Process days using ThreadPoolExecutor for parallelism
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_day = {}
        for day_of_month in range(1, days_in_month + 1):
            cmd = [
                sys.executable, helper_script,
                '--year', str(year),
                '--month', str(month),
                '--day', str(day_of_month)
            ]
            
            if force_scrape:
                cmd.append('--force-scrape')
            
            logger.info(f"Submitting day {day_of_month} for processing")
            future = executor.submit(subprocess.run, cmd, 
                                    capture_output=True, text=True, check=False)
            future_to_day[future] = day_of_month
        
        # Process results as they complete
        for future in as_completed(future_to_day):
            day_of_month = future_to_day[future]
            try:
                result = future.result()
                if result.returncode == 0:
                    logger.info(f"Successfully processed day {day_of_month}")
                    successful_days += 1
                else:
                    logger.error(f"Failed to process day {day_of_month}: {result.stderr}")
            except Exception as e:
                logger.error(f"Exception processing day {day_of_month}: {e}")
    
    # Summarize results
    logger.info(f"Month scraping complete. Successfully processed {successful_days} of {days_in_month} days.")
    
    # Check if any JSON files were created
    json_files = list(json_dir.glob(f"{year}-{month:02d}-*.json"))
    html_files = list(html_dir.glob(f"{year}-{month:02d}-*.html"))
    
    logger.info(f"Generated {len(html_files)} HTML files and {len(json_files)} JSON files")
    
    # Print sample data from one file if available
    if json_files:
        sample_file = json_files[0]
        logger.info(f"Sample data from {sample_file}:")
        try:
            with open(sample_file, 'r') as f:
                sample_data = json.load(f)
                logger.info(f"Sample content: {json.dumps(sample_data, indent=2)[:500]}...")
        except Exception as e:
            logger.error(f"Error reading sample file: {e}")
    
    return successful_days, len(html_files), len(json_files)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape a full month of NC Soccer data')
    parser.add_argument('--year', type=int, required=True, help='Year to scrape')
    parser.add_argument('--month', type=int, required=True, help='Month to scrape')
    parser.add_argument('--force-scrape', action='store_true', help='Force scrape even if already scraped')
    
    args = parser.parse_args()
    
    successful_days, html_count, json_count = scrape_month(args.year, args.month, args.force_scrape)
    
    if successful_days > 0:
        print(f"✅ Scraping completed successfully!")
        print(f"Output directory: {Path(__file__).parent.parent.absolute() / 'output' / 'data'}")
        print(f"Generated {html_count} HTML files and {json_count} JSON files")
        sys.exit(0)
    else:
        print(f"❌ Scraping failed. No days were successfully processed.")
        sys.exit(1)

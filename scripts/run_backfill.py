#!/usr/bin/env python3
"""
Test script for running the Backfill Spider
"""

import os
import sys
import argparse
import subprocess
import logging
from datetime import datetime
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("run_backfill.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_backfill(start_year, start_month, end_year, end_month, force_scrape=False, timeout=900):
    """Run the backfill using the scraping module."""
    
    # Use the test_backfill.py script that works with direct class imports
    command = [
        "python", 
        "scripts/test_backfill.py"
    ]
    
    # Set environment variables for the parameters
    env = os.environ.copy()
    env["START_YEAR"] = str(start_year)
    env["START_MONTH"] = str(start_month)
    if end_year:
        env["END_YEAR"] = str(end_year)
    if end_month:
        env["END_MONTH"] = str(end_month)
    if force_scrape:
        env["FORCE_SCRAPE"] = "true"
    if timeout:
        env["TIMEOUT"] = str(timeout)
    
    # Run the command
    logger.info(f"Running backfill with parameters: {env}")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, env=env)
        logger.info(f"Backfill succeeded with output: {result.stdout}")
        print(f"Backfill completed. See run_backfill.log for details.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Backfill failed with error: {e.stderr}")
        print(f"Backfill failed. See run_backfill.log for details.")
        return False

def main():
    parser = argparse.ArgumentParser(description='Run NC Soccer backfill')
    parser.add_argument('--start-year', type=int, default=2007, 
                        help='Start year (default: 2007)')
    parser.add_argument('--start-month', type=int, default=1, 
                        help='Start month (default: 1)')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, 
                        help='End year (default: current year)')
    parser.add_argument('--end-month', type=int, default=datetime.now().month, 
                        help='End month (default: current month)')
    parser.add_argument('--force-scrape', action='store_true', 
                        help='Force re-scrape even if already done')
    parser.add_argument('--timeout', type=int, default=900, 
                        help='Maximum time to run in seconds (default: 900)')
    
    args = parser.parse_args()
    
    # Validate input
    if args.start_year < 2007:
        logger.warning("Start year is before 2007, setting to 2007")
        args.start_year = 2007
    
    if args.end_year > datetime.now().year:
        logger.warning(f"End year is in the future, setting to current year ({datetime.now().year})")
        args.end_year = datetime.now().year
    
    if args.start_year > args.end_year:
        logger.error("Start year cannot be greater than end year")
        sys.exit(1)
    
    if args.start_year == args.end_year and args.start_month > args.end_month:
        logger.error("Start month cannot be greater than end month within the same year")
        sys.exit(1)
    
    # Start the backfill process
    logger.info(f"Starting backfill from {args.start_year}-{args.start_month:02d} to {args.end_year}-{args.end_month:02d}")
    logger.info(f"Force scrape: {args.force_scrape}")
    logger.info(f"Timeout: {args.timeout} seconds")
    
    success = run_backfill(
        args.start_year,
        args.start_month,
        args.end_year,
        args.end_month,
        args.force_scrape,
        args.timeout
    )
    
    if success:
        logger.info("Backfill process completed successfully")
        sys.exit(0)
    else:
        logger.error("Backfill process failed")
        sys.exit(1)

if __name__ == '__main__':
    main()
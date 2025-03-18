#!/usr/bin/env python3
"""
Test script for unified schedule spider that demonstrates both single date 
and date range scraping using the same spider.
"""
import subprocess
import sys
import os
import time
from datetime import datetime, timedelta

def test_single_date():
    """Test scraping a single date (original mode)"""
    print("Testing single date scraping...")
    
    # Format: today's date
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    
    cmd = [
        "cd", "../../../scraping", "&&", 
        "scrapy", "crawl", "schedule",
        "-a", f"year={year}",
        "-a", f"month={month}",
        "-a", f"day={day}",
        "-a", "force_scrape=true",
        "-a", "storage_type=file",
        "-L", "INFO"
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(" ".join(cmd), shell=True, check=True)

def test_date_range():
    """Test scraping a date range (formerly BackfillSpider)"""
    print("Testing date range scraping...")
    
    # Use mode=range and date range parameters
    now = datetime.now()
    start_date = now - timedelta(days=7)  # One week ago
    
    cmd = [
        "cd", "../../../scraping", "&&", 
        "scrapy", "crawl", "schedule",
        "-a", "mode=range",
        "-a", f"start_year={start_date.year}",
        "-a", f"start_month={start_date.month}",
        "-a", f"start_day={start_date.day}",
        "-a", f"end_year={now.year}",
        "-a", f"end_month={now.month}",
        "-a", f"end_day={now.day}",
        "-a", "force_scrape=true",
        "-a", "storage_type=file",
        "-L", "INFO"
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(" ".join(cmd), shell=True, check=True)

if __name__ == "__main__":
    print("Testing unified schedule spider...")
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "single":
            test_single_date()
        elif sys.argv[1] == "range":
            test_date_range()
        else:
            print(f"Unknown test mode: {sys.argv[1]}")
            print("Valid modes: single, range")
            sys.exit(1)
    else:
        print("Running all tests...")
        test_single_date()
        print("\n")
        test_date_range()

#!/usr/bin/env python3
"""
Pipeline Runner Script

This script orchestrates the entire data processing pipeline from HTML to Parquet.

Usage:
    python run_pipeline.py --bucket ncsh-app-data --dry-run
    python run_pipeline.py --bucket ncsh-app-data --start-date 2022-01-01 --end-date 2022-01-31
"""

import os
import argparse
import logging
import subprocess
import time
import uuid
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run the full data processing pipeline')

    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--html-prefix', default='data/html/', help='Prefix for HTML files')
    parser.add_argument('--json-prefix', default='data/json/', help='Prefix for JSON files')
    parser.add_argument('--parquet-prefix', default='data/parquet/', help='Prefix for Parquet files')
    parser.add_argument('--dataset-name', default='ncsoccer_games', help='Dataset name')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without making changes')
    parser.add_argument('--start-date', help='Start date for processing (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for processing (YYYY-MM-DD)')
    parser.add_argument('--force-reprocess', action='store_true', help='Force reprocessing of files')
    parser.add_argument('--html-only', action='store_true', help='Only process HTML to JSON')
    parser.add_argument('--json-only', action='store_true', help='Only process JSON to Parquet')

    return parser.parse_args()

def run_html_to_json(args):
    """Run the HTML to JSON conversion process."""
    logger.info("Starting HTML to JSON processing")

    run_id = f"run_{int(time.time())}"
    cmd = [
        "python", "scripts/process_html.py",
        "--bucket", args.bucket,
        "--html-prefix", args.html_prefix,
        "--json-prefix", args.json_prefix,
        "--checkpoint-name", "html_processing",
        "--run-id", run_id
    ]

    if args.start_date:
        cmd.extend(["--start-date", args.start_date])

    if args.end_date:
        cmd.extend(["--end-date", args.end_date])

    if args.force_reprocess:
        cmd.append("--force-reprocess")

    if args.dry_run:
        cmd.append("--dry-run")

    # Run the HTML to JSON process
    start_time = time.time()
    logger.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True, text=True, capture_output=True)
    end_time = time.time()

    logger.info(f"HTML to JSON processing completed in {end_time - start_time:.2f} seconds")
    logger.debug(result.stdout)

    if result.stderr:
        logger.warning(f"Warnings during HTML to JSON processing: {result.stderr}")

    return run_id

def run_json_to_parquet(args):
    """Run the JSON to Parquet conversion process."""
    logger.info("Starting JSON to Parquet conversion")

    run_id = f"run_{int(time.time())}"
    cmd = [
        "python", "scripts/json_to_parquet.py",
        "--bucket", args.bucket,
        "--json-prefix", args.json_prefix,
        "--parquet-prefix", args.parquet_prefix,
        "--dataset-name", args.dataset_name,
        "--checkpoint-name", "json_to_parquet",
        "--run-id", run_id
    ]

    if args.start_date:
        cmd.extend(["--start-date", args.start_date])

    if args.end_date:
        cmd.extend(["--end-date", args.end_date])

    if args.force_reprocess:
        cmd.append("--force-reprocess")

    if args.dry_run:
        cmd.append("--dry-run")

    # Run the JSON to Parquet process
    start_time = time.time()
    logger.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True, text=True, capture_output=True)
    end_time = time.time()

    logger.info(f"JSON to Parquet conversion completed in {end_time - start_time:.2f} seconds")
    logger.debug(result.stdout)

    if result.stderr:
        logger.warning(f"Warnings during JSON to Parquet conversion: {result.stderr}")

    return run_id

def main():
    """Main function to run the entire pipeline."""
    args = parse_arguments()

    start_time = time.time()

    # Run HTML to JSON if not json_only
    if not args.json_only:
        html_run_id = run_html_to_json(args)
        logger.info(f"HTML to JSON processing completed with run ID: {html_run_id}")

    # Run JSON to Parquet if not html_only
    if not args.html_only:
        json_run_id = run_json_to_parquet(args)
        logger.info(f"JSON to Parquet conversion completed with run ID: {json_run_id}")

    end_time = time.time()
    logger.info(f"Pipeline run completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
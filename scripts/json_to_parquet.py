#!/usr/bin/env python3
"""
JSON to Parquet Conversion Script

This script converts JSON files from S3 to Parquet format.

Usage:
    python json_to_parquet.py --bucket ncsh-app-data --dry-run
    python json_to_parquet.py --bucket ncsh-app-data
"""

import os
import argparse
import logging
import boto3
import json
import pandas as pd
from datetime import datetime
import time
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO
import tempfile
import uuid

# Import our checkpoint manager
from checkpoint import CheckpointManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Convert JSON files to Parquet')

    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--json-prefix', default='data/json/', help='Prefix for JSON files')
    parser.add_argument('--parquet-prefix', default='data/parquet/', help='Prefix for Parquet files')
    parser.add_argument('--dataset-name', default='ncsoccer_games', help='Dataset name')
    parser.add_argument('--checkpoint-name', default='json_to_parquet', help='Checkpoint name')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without making changes')
    parser.add_argument('--start-date', help='Start date for processing (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for processing (YYYY-MM-DD)')
    parser.add_argument('--force-reprocess', action='store_true', help='Force reprocessing of files')
    parser.add_argument('--run-id', help='Optional run ID for this processing run')

    return parser.parse_args()

def list_json_files(bucket, prefix, start_date=None, end_date=None, checkpoint_manager=None):
    """List JSON files in S3 that haven't been processed yet."""
    s3 = boto3.client('s3')
    json_files = []

    # List all JSON files recursively
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    date_pattern = r'year=(\d{4})/month=(\d{2})/day=(\d{2})'

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.json'):
                    # Extract date from key using the path structure
                    import re
                    match = re.search(date_pattern, key)
                    if match:
                        year, month, day = match.groups()
                        file_date = f"{year}-{month}-{day}"

                        # Filter by date range if specified
                        if start_date and file_date < start_date:
                            continue
                        if end_date and file_date > end_date:
                            continue

                        # Skip files already processed unless force_reprocess
                        if checkpoint_manager and not checkpoint_manager.is_file_processed(key):
                            json_files.append({
                                'key': key,
                                'date': file_date
                            })
                        elif checkpoint_manager is None:  # force_reprocess case
                            json_files.append({
                                'key': key,
                                'date': file_date
                            })

    # Sort by date
    json_files.sort(key=lambda x: x['date'])

    return json_files

def process_json_files(s3, bucket, json_files, parquet_prefix, dataset_name, dry_run=False):
    """Process JSON files and convert to Parquet."""
    if not json_files:
        logger.info("No JSON files to process")
        return None, 0

    # Create a timestamp for this processing run
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    # Initialize empty DataFrame to hold all data
    all_data = pd.DataFrame()
    processed_paths = []

    # Process each JSON file
    for i, file_info in enumerate(json_files):
        try:
            key = file_info['key']

            # Get JSON data
            response = s3.get_object(Bucket=bucket, Key=key)
            json_content = response['Body'].read().decode('utf-8')

            # Parse JSON
            data = json.loads(json_content)

            if not data:
                logger.warning(f"No data in {key}")
                continue

            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Add metadata
            df['source_file'] = key
            df['processed_date'] = datetime.utcnow().isoformat()

            # Append to main DataFrame
            all_data = pd.concat([all_data, df], ignore_index=True)
            processed_paths.append(key)

            if (i+1) % 100 == 0 or i == len(json_files) - 1:
                logger.info(f"Processed {i+1}/{len(json_files)} JSON files")

        except Exception as e:
            logger.error(f"Error processing {key}: {str(e)}")

    if all_data.empty:
        logger.warning("No data collected from JSON files")
        return None, 0

    # Save as Parquet
    if not dry_run:
        try:
            # Convert DataFrame to PyArrow Table
            table = pa.Table.from_pandas(all_data)

            # Create temporary file
            with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
                # Write to temporary file
                pq.write_table(table, tmp_file.name)

                # Upload to S3 with timestamp
                timestamped_key = f"{parquet_prefix}{dataset_name}_{timestamp}.parquet"
                latest_key = f"{parquet_prefix}{dataset_name}_latest.parquet"

                # Upload timestamped version
                s3.upload_file(
                    tmp_file.name,
                    bucket,
                    timestamped_key
                )
                logger.info(f"Uploaded Parquet file to s3://{bucket}/{timestamped_key}")

                # Upload as "latest" version
                s3.upload_file(
                    tmp_file.name,
                    bucket,
                    latest_key
                )
                logger.info(f"Updated latest Parquet file at s3://{bucket}/{latest_key}")

                return timestamp, len(processed_paths)
        except Exception as e:
            logger.error(f"Error saving Parquet file: {str(e)}")
            return None, 0
    else:
        logger.info(f"DRY RUN: Would create Parquet file with {len(all_data)} rows from {len(processed_paths)} files")
        return timestamp, len(processed_paths)

def main():
    """Main function."""
    args = parse_arguments()

    # Initialize S3 client
    s3 = boto3.client('s3')

    # Initialize checkpoint manager
    checkpoint_manager = CheckpointManager(
        bucket=args.bucket,
        checkpoint_name=args.checkpoint_name
    )

    # Initialize or read checkpoint
    checkpoint = checkpoint_manager.initialize_checkpoint(dry_run=args.dry_run)

    # List JSON files that haven't been processed yet
    logger.info(f"Listing JSON files in s3://{args.bucket}/{args.json_prefix}")

    # If force_reprocess is True, don't use checkpoint for filtering
    check_manager = None if args.force_reprocess else checkpoint_manager

    json_files = list_json_files(
        args.bucket,
        args.json_prefix,
        args.start_date,
        args.end_date,
        check_manager  # Pass checkpoint manager to filter already processed files
    )

    logger.info(f"Found {len(json_files)} JSON files to process")

    # Process JSON files
    timestamp, processed_count = process_json_files(
        s3,
        args.bucket,
        json_files,
        args.parquet_prefix,
        args.dataset_name,
        args.dry_run
    )

    # Update checkpoint
    if not args.dry_run and processed_count > 0:
        processed_paths = [file_info['key'] for file_info in json_files[:processed_count]]

        checkpoint_data = {
            "files_processed": processed_count,
            "success_count": processed_count,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "processed_paths": processed_paths,
            "run_id": args.run_id if args.run_id else None,
            "timestamp": timestamp
        }

        checkpoint_manager.update_checkpoint(checkpoint_data, dry_run=args.dry_run)
        logger.info(f"Updated checkpoint with {len(processed_paths)} new processed paths")

    logger.info("Processing complete")

if __name__ == "__main__":
    main()
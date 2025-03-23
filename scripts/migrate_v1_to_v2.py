#!/usr/bin/env python3

import argparse
import boto3
import logging
import os
import json
from datetime import datetime
import re
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_date_from_v1_key(key):
    """
    Extract date from v1 key structure which uses year/month/day folders
    """
    # Look for the year/month/day pattern in the path
    path_pattern = r'year=(\d{4})/month=(\d{2})/day=(\d{2})'
    match = re.search(path_pattern, key)

    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"

    # If the pattern isn't found using the specific format
    # Try a more general date pattern extraction
    date_pattern = r'(\d{4})-(\d{2})-(\d{2})'
    match = re.search(date_pattern, key)
    if match:
        return match.group(0)

    logging.warning(f"Could not extract date from {key}, skipping")
    return None

def list_v1_files(s3_client, bucket, prefix='data/json/'):
    """List all JSON files in the v1 structure"""
    v1_files = []

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            key = obj['Key']
            if key.endswith('.json'):
                v1_files.append(key)

    return v1_files

def migrate_file(s3_client, bucket, v1_key, v2_key, execute=False):
    """Copy file from v1 to v2 structure"""
    if execute:
        try:
            copy_source = {'Bucket': bucket, 'Key': v1_key}
            s3_client.copy(copy_source, bucket, v2_key)
            logging.info(f"Migrated {v1_key} to {v2_key}")
            return True
        except ClientError as e:
            logging.error(f"Failed to migrate {v1_key}: {str(e)}")
            return False
    else:
        logging.debug(f"Would migrate {v1_key} to {v2_key}")
        return True

def migrate_json_files(s3_client, bucket, execute=False):
    """Migrate JSON files from v1 to v2 structure"""
    v1_files = list_v1_files(s3_client, bucket)
    migrated_count = 0
    skipped_count = 0

    for v1_key in v1_files:
        date_str = get_date_from_v1_key(v1_key)
        if date_str:
            # Structure for v2: v2/processed/json/year=YYYY/month=MM/day=DD/YYYY-MM-DD_games.jsonl
            # or v2/processed/json/year=YYYY/month=MM/day=DD/YYYY-MM-DD_meta.json

            # Check if it's likely a games file or a metadata file
            is_meta = "meta" in v1_key.lower() or "metadata" in v1_key.lower()
            file_suffix = "meta.json" if is_meta else "games.jsonl"

            year, month, day = date_str.split('-')
            v2_key = f"v2/processed/json/year={year}/month={month}/day={day}/{date_str}_{file_suffix}"

            if migrate_file(s3_client, bucket, v1_key, v2_key, execute):
                migrated_count += 1
        else:
            skipped_count += 1

    logging.info(f"Migration summary: {migrated_count} files would be migrated, {skipped_count} files skipped")
    if not execute:
        logging.info("This was a dry run. Use --execute to perform the actual migration.")

    return migrated_count, skipped_count

def migrate_parquet_data(s3_client, bucket, execute=False):
    """Check for and migrate Parquet data from v1 to v2 structure"""
    logging.info("Checking for existing Parquet files in v1 structure")

    # Check for the main Parquet file in v1
    try:
        s3_client.head_object(Bucket=bucket, Key="data/parquet/data.parquet")
        logging.info("Found data.parquet in v1 structure")

        # Create a timestamp for the backup
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        v2_parquet_key = f"v2/processed/parquet/{timestamp}/data.parquet"

        logging.info(f"Would backup v1 data.parquet to v2 structure with timestamp")

        if execute:
            copy_source = {'Bucket': bucket, 'Key': "data/parquet/data.parquet"}
            s3_client.copy(copy_source, bucket, v2_parquet_key)
            # Also copy to the latest version
            s3_client.copy(copy_source, bucket, "v2/processed/parquet/data.parquet")
            logging.info(f"Backed up v1 data.parquet to {v2_parquet_key} and v2/processed/parquet/data.parquet")
            return True

    except ClientError:
        logging.info("No data.parquet found in v1 structure")

    return False

def main():
    parser = argparse.ArgumentParser(description='Migrate data from v1 to v2 architecture')
    parser.add_argument('--bucket', default='ncsh-app-data', help='AWS S3 bucket name')
    parser.add_argument('--profile', default=None, help='AWS profile to use')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--execute', action='store_true', help='Execute the migration (without this flag, it\'s a dry run)')

    args = parser.parse_args()

    # Set up AWS session
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3_client = session.client('s3')

    # Migrate the JSON files
    migrate_json_files(s3_client, args.bucket, args.execute)

    # Check for and migrate Parquet data
    migrate_parquet_data(s3_client, args.bucket, args.execute)

    if not args.execute:
        logging.info("Dry run completed. Use --execute to perform the actual migration.")

if __name__ == "__main__":
    main()
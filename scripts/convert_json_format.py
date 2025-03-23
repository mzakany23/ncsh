#!/usr/bin/env python3
"""
Utility script to convert JSON format from old backfill format to the format expected by processing Lambda.
This script:
1. Reads JSON files from a source directory
2. Converts them to the expected format
3. Saves them to a destination directory
4. Optionally triggers the processing Lambda to convert to Parquet
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime
import boto3
import glob
from typing import List, Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def convert_json_format(source_file: str, dest_file: str) -> int:
    """
    Convert JSON format from old backfill format to processing Lambda format.

    Args:
        source_file (str): Path to source JSON file
        dest_file (str): Path to destination JSON file

    Returns:
        int: Number of games converted
    """
    try:
        # Read source JSON
        with open(source_file, 'r') as f:
            source_data = json.load(f)

        # Skip meta files
        if source_file.endswith('_meta.json'):
            logger.debug(f"Skipping meta file: {source_file}")
            return 0

        # Convert format
        converted_data = []
        games_count = 0

        if isinstance(source_data, dict) and 'games' in source_data:
            # This is already in the expected format with 'games' array
            games = source_data.get('games', [])
            date_str = source_data.get('date')
            games_count = len(games)

            # Create converted data in the expected format
            for game in games:
                game_data = {
                    'date': date_str,
                    'home_team': game.get('home_team', ''),
                    'away_team': game.get('away_team', ''),
                    'home_score': game.get('home_score'),
                    'away_score': game.get('away_score'),
                    'league': game.get('league', ''),
                    'time': game.get('time'),
                    'url': None,
                    'type': None,
                    'status': None,
                    'headers': None,
                    'timestamp': datetime.now().isoformat()
                }
                converted_data.append(game_data)

        elif isinstance(source_data, list):
            # This is the format from the previous implementation
            games_count = len(source_data)
            date_str = os.path.basename(source_file).replace('.json', '')

            # Create converted data in the expected format
            for game in source_data:
                game_data = {
                    'date': date_str,
                    'home_team': game.get('home_team', ''),
                    'away_team': game.get('away_team', ''),
                    'home_score': game.get('home_score'),
                    'away_score': game.get('away_score'),
                    'league': game.get('league_name', ''),
                    'time': game.get('time'),
                    'url': None,
                    'type': None,
                    'status': None,
                    'headers': None,
                    'timestamp': datetime.now().isoformat()
                }
                converted_data.append(game_data)

        # Write converted data
        os.makedirs(os.path.dirname(dest_file), exist_ok=True)
        with open(dest_file, 'w') as f:
            json.dump(converted_data, f, indent=2)

        logger.info(f"Converted {games_count} games from {source_file} to {dest_file}")
        return games_count

    except Exception as e:
        logger.error(f"Error converting {source_file}: {e}")
        return 0

def process_directory(source_dir: str, dest_dir: str) -> Dict[str, int]:
    """
    Process all JSON files in a directory.

    Args:
        source_dir (str): Source directory with JSON files
        dest_dir (str): Destination directory for converted JSON files

    Returns:
        Dict[str, int]: Statistics about the conversion
    """
    stats = {
        'total_files': 0,
        'converted_files': 0,
        'total_games': 0,
        'errors': 0
    }

    # Find all JSON files
    json_files = glob.glob(os.path.join(source_dir, '**/*.json'), recursive=True)
    stats['total_files'] = len(json_files)

    # Convert each file
    for source_file in json_files:
        if '_meta.json' in source_file:
            continue  # Skip meta files

        # Determine destination file path
        rel_path = os.path.relpath(source_file, source_dir)
        dest_file = os.path.join(dest_dir, rel_path)

        try:
            games_count = convert_json_format(source_file, dest_file)
            if games_count > 0:
                stats['converted_files'] += 1
                stats['total_games'] += games_count
        except Exception as e:
            logger.error(f"Error processing {source_file}: {e}")
            stats['errors'] += 1

    return stats

def upload_to_s3(local_dir: str, bucket: str, prefix: str) -> List[str]:
    """
    Upload converted JSON files to S3.

    Args:
        local_dir (str): Local directory with converted JSON files
        bucket (str): S3 bucket name
        prefix (str): S3 key prefix

    Returns:
        List[str]: List of uploaded S3 keys
    """
    s3 = boto3.client('s3')
    uploaded_keys = []

    # Find all JSON files
    json_files = glob.glob(os.path.join(local_dir, '**/*.json'), recursive=True)
    logger.info(f"Found {len(json_files)} JSON files to upload")

    # Upload each file
    for local_file in json_files:
        if '_meta.json' in local_file:
            continue  # Skip meta files

        # Determine S3 key
        rel_path = os.path.relpath(local_file, local_dir)
        s3_key = os.path.join(prefix, rel_path)

        try:
            logger.info(f"Uploading {local_file} to s3://{bucket}/{s3_key}")
            s3.upload_file(local_file, bucket, s3_key)
            uploaded_keys.append(s3_key)
        except Exception as e:
            logger.error(f"Error uploading {local_file}: {e}")

    return uploaded_keys

def trigger_processing(bucket: str, files: List[str], dst_bucket: str, dst_prefix: str) -> Dict:
    """
    Trigger the processing Lambda to convert JSON to Parquet.

    Args:
        bucket (str): Source S3 bucket with JSON files
        files (List[str]): List of S3 keys for JSON files
        dst_bucket (str): Destination S3 bucket for Parquet files
        dst_prefix (str): Destination S3 key prefix for Parquet files

    Returns:
        Dict: Response from Lambda invocation
    """
    lambda_client = boto3.client('lambda')

    # Prepare the event
    event = {
        'operation': 'convert',
        'src_bucket': bucket,
        'files': files,
        'dst_bucket': dst_bucket,
        'dst_prefix': dst_prefix
    }

    try:
        logger.info(f"Invoking processing Lambda with {len(files)} files")
        response = lambda_client.invoke(
            FunctionName='ncsoccer-processing',
            InvocationType='RequestResponse',
            Payload=json.dumps(event)
        )

        # Parse response
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        logger.info(f"Processing complete: {response_payload}")
        return response_payload

    except Exception as e:
        logger.error(f"Error invoking processing Lambda: {e}")
        return {'status': 'ERROR', 'message': str(e)}

def main():
    """Main function for the script."""
    parser = argparse.ArgumentParser(description='Convert JSON format from old backfill format to processing Lambda')
    parser.add_argument('--source-dir', required=True, help='Source directory with JSON files')
    parser.add_argument('--dest-dir', required=True, help='Destination directory for converted JSON files')
    parser.add_argument('--upload', action='store_true', help='Upload converted files to S3')
    parser.add_argument('--bucket', help='S3 bucket for upload/processing')
    parser.add_argument('--prefix', default='data/json', help='S3 key prefix for upload')
    parser.add_argument('--process', action='store_true', help='Trigger processing Lambda')
    parser.add_argument('--dst-bucket', help='Destination S3 bucket for Parquet files')
    parser.add_argument('--dst-prefix', default='data/parquet/', help='Destination S3 key prefix for Parquet files')

    args = parser.parse_args()

    # Process directory
    logger.info(f"Converting files from {args.source_dir} to {args.dest_dir}")
    stats = process_directory(args.source_dir, args.dest_dir)
    logger.info(f"Conversion complete: {stats}")

    # Upload to S3 if requested
    if args.upload:
        if not args.bucket:
            logger.error("--bucket is required when --upload is specified")
            sys.exit(1)

        logger.info(f"Uploading files to s3://{args.bucket}/{args.prefix}")
        uploaded_keys = upload_to_s3(args.dest_dir, args.bucket, args.prefix)
        logger.info(f"Upload complete: {len(uploaded_keys)} files")

        # Trigger processing if requested
        if args.process:
            if not args.dst_bucket:
                args.dst_bucket = args.bucket
                logger.info(f"Using source bucket as destination bucket: {args.dst_bucket}")

            logger.info(f"Triggering processing to convert to Parquet format")
            result = trigger_processing(args.bucket, uploaded_keys, args.dst_bucket, args.dst_prefix)
            logger.info(f"Processing triggered: {result}")

    # Exit
    sys.exit(0)

if __name__ == '__main__':
    main()
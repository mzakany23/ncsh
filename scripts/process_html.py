#!/usr/bin/env python3
"""
Process HTML Script

This script processes HTML files from S3 and converts them into JSON in our new partitioned format.

Usage:
    python process_html.py --bucket ncsh-app-data --dry-run
    python process_html.py --bucket ncsh-app-data
"""

import os
import argparse
import logging
import boto3
import json
import pandas as pd
import re
from datetime import datetime
from bs4 import BeautifulSoup
from botocore.exceptions import ClientError
from io import StringIO

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
    parser = argparse.ArgumentParser(description='Process HTML files')

    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--html-prefix', default='data/html/', help='Prefix for HTML files')
    parser.add_argument('--json-prefix', default='data/json/', help='Prefix for JSON files')
    parser.add_argument('--checkpoint-name', default='html_processing', help='Checkpoint name')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without making changes')
    parser.add_argument('--limit', type=int, help='Limit processing to this many files')
    parser.add_argument('--start-date', help='Start date for processing (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for processing (YYYY-MM-DD)')
    parser.add_argument('--force-reprocess', action='store_true', help='Force reprocessing of files')
    parser.add_argument('--run-id', help='Optional run ID for this processing run')

    return parser.parse_args()

def list_html_files(bucket, prefix, start_date=None, end_date=None, checkpoint_manager=None):
    """List HTML files in S3 that haven't been processed yet."""
    s3 = boto3.client('s3')
    html_files = []

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.html'):
                    # Extract date from filename
                    filename = os.path.basename(key)
                    date_match = re.match(r'(\d{4}-\d{2}-\d{2})\.html', filename)

                    if date_match:
                        file_date = date_match.group(1)

                        # Filter by date range if specified
                        if start_date and file_date < start_date:
                            continue
                        if end_date and file_date > end_date:
                            continue

                        # Skip files already processed unless checkpoint_manager is None
                        if checkpoint_manager:
                            # Get corresponding JSON path
                            year, month, day = file_date.split('-')
                            json_key = f"data/json/year={year}/month={month}/day={day}/data.json"

                            # Check if this file has already been processed
                            if checkpoint_manager.is_file_processed(json_key):
                                logger.debug(f"Skipping already processed file: {key}")
                                continue

                        html_files.append({
                            'key': key,
                            'date': file_date
                        })

    # Sort by date
    html_files.sort(key=lambda x: x['date'])

    return html_files

def process_html_file(s3_client, bucket, file_info, json_prefix, dry_run=False):
    """Process a single HTML file and convert to JSON."""
    key = file_info['key']
    date = file_info['date']

    try:
        # Get HTML file
        response = s3_client.get_object(Bucket=bucket, Key=key)
        html_content = response['Body'].read().decode('utf-8')

        # Parse HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract data from HTML
        games_data = extract_games_from_html(soup, date)

        if not games_data:
            logger.warning(f"No games found in {key}")
            return None

        # Create partitioned path
        year, month, day = date.split('-')
        json_key = f"{json_prefix}year={year}/month={month}/day={day}/data.json"

        # Upload JSON data
        if not dry_run:
            s3_client.put_object(
                Bucket=bucket,
                Key=json_key,
                Body=json.dumps(games_data, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Processed {key} -> {json_key} with {len(games_data)} games")
        else:
            logger.info(f"DRY RUN: Would process {key} -> {json_key} with {len(games_data)} games")

        return json_key

    except Exception as e:
        logger.error(f"Error processing {key}: {str(e)}")
        return None

def extract_games_from_html(soup, date_str):
    """Extract game data from HTML."""
    games = []

    try:
        # Look for the "Events on" header
        events_headers = soup.find_all(string=lambda text: text and "Events on" in text)

        for header in events_headers:
            # Find the closest table to this header
            table = None
            parent = header.parent

            # Search up to 5 levels up from the header for a table
            for _ in range(5):
                if parent is None:
                    break

                # Try to find a table within this parent
                tables = parent.find_all('table', class_='ezl-base-table')
                if tables:
                    table = tables[0]
                    break

                # Move up one level
                parent = parent.parent

            if table:
                # Find date from header if available (format: "Events on Weekday, Month Day, Year")
                if events_headers and len(events_headers) > 0:
                    header_text = events_headers[0].strip()
                    date_parts = header_text.split(',')
                    if len(date_parts) >= 2:
                        # We're using the file date for now, but this could be updated if needed
                        pass

                # Process rows in the table
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        try:
                            start_time = cells[0].text.strip()
                            end_time = cells[1].text.strip()
                            description = cells[2].text.strip()
                            field = cells[3].text.strip()

                            # Try to extract teams from description
                            teams = description.split(' vs ')
                            home_team = teams[0] if len(teams) > 0 else description
                            away_team = teams[1] if len(teams) > 1 else "Unknown"

                            # If no vs found, try other formats (like "TeamA - TeamB")
                            if len(teams) == 1:
                                teams = description.split(' - ')
                                home_team = teams[0] if len(teams) > 0 else description
                                away_team = teams[1] if len(teams) > 1 else "Unknown"

                            # Still just using the description if no teams found
                            if home_team == description and ' ' in description:
                                # Try to guess - some might be practice slots
                                if "Practice" in description:
                                    home_team = description
                                    away_team = "Practice"

                            # Create game object
                            game = {
                                'date': date_str,
                                'start_time': start_time,
                                'end_time': end_time,
                                'field': field,
                                'description': description,
                                'home_team': home_team,
                                'away_team': away_team,
                                'timestamp': datetime.utcnow().isoformat()
                            }

                            games.append(game)
                        except Exception as e:
                            logger.warning(f"Error parsing row: {str(e)}")

        # If we still don't have games, try looking for any table with class 'ezl-base-table'
        if not games:
            tables = soup.find_all('table', class_='ezl-base-table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        try:
                            # Skip header rows
                            if cells[0].name == 'th':
                                continue

                            start_time = cells[0].text.strip()
                            end_time = cells[1].text.strip() if len(cells) > 1 else ""
                            description = cells[2].text.strip() if len(cells) > 2 else ""
                            field = cells[3].text.strip() if len(cells) > 3 else ""

                            # Try to extract teams from description
                            teams = description.split(' vs ')
                            home_team = teams[0] if len(teams) > 0 else description
                            away_team = teams[1] if len(teams) > 1 else "Unknown"

                            # Create game object
                            game = {
                                'date': date_str,
                                'start_time': start_time,
                                'end_time': end_time,
                                'field': field,
                                'description': description,
                                'home_team': home_team,
                                'away_team': away_team,
                                'timestamp': datetime.utcnow().isoformat()
                            }

                            games.append(game)
                        except Exception as e:
                            logger.warning(f"Error parsing row: {str(e)}")

    except Exception as e:
        logger.error(f"Error extracting games: {str(e)}")

    return games

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

    # List HTML files that haven't been processed yet
    logger.info(f"Listing HTML files in s3://{args.bucket}/{args.html_prefix}")

    # If force_reprocess is True, don't use checkpoint for filtering
    check_manager = None if args.force_reprocess else checkpoint_manager

    html_files = list_html_files(
        args.bucket,
        args.html_prefix,
        args.start_date,
        args.end_date,
        check_manager  # Pass checkpoint manager to filter already processed files
    )
    logger.info(f"Found {len(html_files)} HTML files to process")

    # Limit files if specified
    if args.limit and len(html_files) > args.limit:
        html_files = html_files[:args.limit]
        logger.info(f"Limited to {len(html_files)} files")

    # Process each file
    processed_count = 0
    success_count = 0
    processed_paths = []

    for file_info in html_files:
        processed_count += 1
        json_path = process_html_file(s3, args.bucket, file_info, args.json_prefix, args.dry_run)

        if json_path:
            success_count += 1
            processed_paths.append(json_path)

        if processed_count % 100 == 0:
            logger.info(f"Processed {processed_count}/{len(html_files)} files...")

    logger.info(f"Processing complete. Processed {processed_count} files, {success_count} successful.")

    # Update checkpoint
    if not args.dry_run and processed_count > 0:
        checkpoint_data = {
            "files_processed": processed_count,
            "success_count": success_count,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "processed_paths": processed_paths,
            "run_id": args.run_id if args.run_id else None
        }

        checkpoint_manager.update_checkpoint(checkpoint_data, dry_run=args.dry_run)
        logger.info(f"Updated checkpoint with {len(processed_paths)} new processed paths")

if __name__ == "__main__":
    main()
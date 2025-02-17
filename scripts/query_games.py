#!/usr/bin/env python3
"""Query games data from S3 using DuckDB."""

import os
import sys
import json
import argparse
import logging
import duckdb
import boto3
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def setup_duckdb():
    """Set up DuckDB with S3 integration."""
    # Create connection
    con = duckdb.connect()

    # Install and load httpfs extension for S3 access
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Configure URL encoding
    con.execute("SET enable_http_metadata_cache=true;")
    con.execute("SET enable_object_cache=true;")
    con.execute("SET binary_as_string=false;")

    # Set AWS credentials if using a specific profile
    if 'AWS_PROFILE' in os.environ:
        session = boto3.Session(profile_name=os.environ['AWS_PROFILE'])
        credentials = session.get_credentials()
        con.execute(f"""
            SET s3_region='us-east-2';
            SET s3_access_key_id='{credentials.access_key}';
            SET s3_secret_access_key='{credentials.secret_key}';
            SET s3_session_token='{credentials.token if credentials.token else ''}';
        """)

    return con

def query_games(con, year=None, month=None, day=None):
    """Query games data from S3 using DuckDB."""
    # Build date conditions
    conditions = []
    if year:
        conditions.append(f"year={year}")
    if month:
        conditions.append(f"month={month:02d}")
    if day:
        conditions.append(f"day={day:02d}")

    # Build the path pattern
    path_pattern = "read_json_auto('s3://ncsh-app-data/data/games/year=*/month=*/day=*/data.jsonl')"
    if conditions:
        path = f"s3://ncsh-app-data/data/games/{'/'.join(conditions)}/data.jsonl"
        path_pattern = f"read_json_auto('{path}')"

    # Create a view of the games data
    con.execute(f"""
        CREATE OR REPLACE VIEW games AS
        SELECT *
        FROM {path_pattern}
    """)

    # Query the view
    result = con.execute("""
        SELECT
            league,
            session,
            home_team,
            away_team,
            status,
            venue,
            officials,
            time,
            home_score,
            away_score
        FROM games
        ORDER BY league, home_team
    """).fetchdf()

    return result

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Query games data from S3 using DuckDB')
    parser.add_argument('--year', type=int, help='Filter by year')
    parser.add_argument('--month', type=int, help='Filter by month')
    parser.add_argument('--day', type=int, help='Filter by day')
    parser.add_argument('--output', choices=['table', 'json', 'csv'], default='table',
                      help='Output format (default: table)')
    args = parser.parse_args()

    try:
        # Set up DuckDB
        con = setup_duckdb()

        # Query the data
        result = query_games(con, args.year, args.month, args.day)

        # Output the results
        if args.output == 'json':
            print(result.to_json(orient='records', indent=2))
        elif args.output == 'csv':
            print(result.to_csv(index=False))
        else:  # table
            print(result.to_string(index=False))

    except Exception as e:
        logger.error(f"Error querying data: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
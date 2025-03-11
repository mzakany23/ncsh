#!/usr/bin/env python3
"""
Local backfill runner that performs a full backfill process:
1. Runs the backfill spider to scrape data
2. Converts the JSON format to match the processing Lambda
3. Processes the data into Parquet format
4. Validates the results
"""

import os
import sys
import argparse
import logging
import subprocess
import json
from datetime import datetime
import pandas as pd
import io
from typing import Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_command(cmd: str) -> bool:
    """Run a shell command and log the output."""
    logger.info(f"Running command: {cmd}")
    try:
        process = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        
        # Stream output as it becomes available
        for line in process.stdout:
            print(line.strip())
            
        process.wait()
        return process.returncode == 0
    except Exception as e:
        logger.error(f"Command failed: {e}")
        return False

def run_backfill(start_year: int, start_month: int, end_year: int, end_month: int, 
                base_dir: str, force_scrape: bool = False) -> bool:
    """Run the backfill spider."""
    # Create output directories
    raw_dir = os.path.join(base_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    
    # Build command
    cmd = (
        f"cd {os.path.dirname(os.path.dirname(os.path.abspath(__file__)))} && "
        f"python scripts/test_backfill.py "
        f"--start-year {start_year} --start-month {start_month} "
        f"--end-year {end_year} --end-month {end_month} "
        f"--output-dir {raw_dir} "
        f"{'--force-scrape' if force_scrape else ''}"
    )
    
    return run_command(cmd)

def convert_format(raw_dir: str, converted_dir: str) -> bool:
    """Convert the JSON format to match the processing Lambda."""
    # Create output directory
    os.makedirs(converted_dir, exist_ok=True)
    
    # Build command
    cmd = (
        f"cd {os.path.dirname(os.path.dirname(os.path.abspath(__file__)))} && "
        f"python scripts/convert_json_format.py "
        f"--source-dir {raw_dir} --dest-dir {converted_dir}"
    )
    
    return run_command(cmd)

def convert_to_parquet(converted_dir: str, parquet_dir: str) -> Dict[str, Any]:
    """Convert the JSON data to Parquet format using Pandas."""
    logger.info(f"Converting JSON to Parquet: {converted_dir} -> {parquet_dir}")
    
    try:
        # Create output directory
        os.makedirs(parquet_dir, exist_ok=True)
        
        # Find all JSON files
        json_files = []
        for root, dirs, files in os.walk(converted_dir):
            for file in files:
                if file.endswith('.json') and not file.endswith('_meta.json'):
                    json_files.append(os.path.join(root, file))
        
        logger.info(f"Found {len(json_files)} JSON files to convert")
        
        # Read all JSON files
        all_data = []
        rows_processed = 0
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    
                if isinstance(data, list):
                    all_data.extend(data)
                    rows_processed += len(data)
                    logger.info(f"Processed {len(data)} records from {json_file}")
            except Exception as e:
                logger.error(f"Error processing {json_file}: {e}")
        
        if not all_data:
            logger.error("No data found in JSON files")
            return {"status": "ERROR", "message": "No data found"}
        
        # Convert to DataFrame
        logger.info(f"Creating DataFrame with {len(all_data)} records")
        df = pd.DataFrame(all_data)
        
        # Convert date column to timestamp
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            
        # Convert timestamp column to timestamp
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
        # Convert score columns to numeric
        if 'home_score' in df.columns:
            df['home_score'] = pd.to_numeric(df['home_score'], errors='coerce')
            
        if 'away_score' in df.columns:
            df['away_score'] = pd.to_numeric(df['away_score'], errors='coerce')
        
        # Write to Parquet
        parquet_file = os.path.join(parquet_dir, "data.parquet")
        df.to_parquet(parquet_file, index=False)
        logger.info(f"Wrote {len(df)} records to {parquet_file}")
        
        # Return statistics
        stats = {
            "status": "SUCCESS",
            "rows_processed": len(df),
            "date_range": f"{df['date'].min()} to {df['date'].max()}",
            "unique_dates": df['date'].nunique(),
            "years": sorted(df['date'].dt.year.unique().tolist())
        }
        
        return stats
    
    except Exception as e:
        logger.error(f"Error converting to Parquet: {e}")
        return {"status": "ERROR", "message": str(e)}

def analyze_parquet(parquet_file: str) -> Dict[str, Any]:
    """Analyze the Parquet file to verify the data."""
    logger.info(f"Analyzing Parquet file: {parquet_file}")
    
    try:
        # Read Parquet file
        df = pd.read_parquet(parquet_file)
        logger.info(f"Read {len(df)} records from {parquet_file}")
        
        # Basic statistics
        stats = {
            "total_rows": len(df),
            "date_range": f"{df['date'].min()} to {df['date'].max()}",
            "unique_dates": df['date'].nunique(),
            "unique_teams": len(set(df['home_team'].tolist() + df['away_team'].tolist())),
            "games_with_scores": (~df['home_score'].isna() & ~df['away_score'].isna()).sum(),
            "years_covered": sorted(df['date'].dt.year.unique().tolist()),
            "games_by_year": df.groupby(df['date'].dt.year).size().to_dict(),
            "games_by_month": df.groupby([df['date'].dt.year, df['date'].dt.month]).size().to_dict()
        }
        
        # Check for missing values
        missing_values = df.isna().sum().to_dict()
        stats["missing_values"] = missing_values
        
        # Write statistics to file
        stats_file = os.path.join(os.path.dirname(parquet_file), "analysis.json")
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        logger.info(f"Wrote analysis to {stats_file}")
        
        return stats
    
    except Exception as e:
        logger.error(f"Error analyzing Parquet file: {e}")
        return {"status": "ERROR", "message": str(e)}

def main():
    """Main function to run the full local backfill pipeline."""
    parser = argparse.ArgumentParser(description='Run a full local backfill pipeline')
    parser.add_argument('--start-year', type=int, default=2023, help='Start year (default: 2023)')
    parser.add_argument('--start-month', type=int, default=1, help='Start month (default: 1)')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='End year (default: current year)')
    parser.add_argument('--end-month', type=int, default=datetime.now().month, help='End month (default: current month)')
    parser.add_argument('--output-dir', default='backfill_test', help='Output directory for all data')
    parser.add_argument('--force-scrape', action='store_true', help='Force re-scrape even if already done')
    parser.add_argument('--skip-scrape', action='store_true', help='Skip scraping step (use existing data)')
    parser.add_argument('--skip-convert', action='store_true', help='Skip format conversion step')
    
    args = parser.parse_args()
    
    # Create output directories
    base_dir = os.path.abspath(args.output_dir)
    raw_dir = os.path.join(base_dir, "raw")
    converted_dir = os.path.join(base_dir, "converted")
    parquet_dir = os.path.join(base_dir, "parquet")
    
    # 1. Run backfill spider
    if not args.skip_scrape:
        logger.info("Step 1: Running backfill spider")
        if not run_backfill(args.start_year, args.start_month, args.end_year, args.end_month, base_dir, args.force_scrape):
            logger.error("Backfill failed")
            return 1
    else:
        logger.info("Skipping scrape step")
    
    # 2. Convert format
    if not args.skip_convert:
        logger.info("Step 2: Converting JSON format")
        if not convert_format(os.path.join(raw_dir, "json"), converted_dir):
            logger.error("Format conversion failed")
            return 1
    else:
        logger.info("Skipping format conversion step")
    
    # 3. Convert to Parquet
    logger.info("Step 3: Converting to Parquet")
    parquet_stats = convert_to_parquet(converted_dir, parquet_dir)
    if parquet_stats.get("status") != "SUCCESS":
        logger.error(f"Parquet conversion failed: {parquet_stats.get('message')}")
        return 1
    
    # 4. Analyze results
    logger.info("Step 4: Analyzing results")
    analysis = analyze_parquet(os.path.join(parquet_dir, "data.parquet"))
    
    # Print summary
    logger.info("Backfill pipeline complete!")
    logger.info(f"Total records: {analysis.get('total_rows', 0)}")
    logger.info(f"Date range: {analysis.get('date_range', 'unknown')}")
    logger.info(f"Unique dates: {analysis.get('unique_dates', 0)}")
    logger.info(f"Years covered: {analysis.get('years_covered', [])}")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
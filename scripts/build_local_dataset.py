#!/usr/bin/env python3
"""
Build a consolidated Parquet dataset from local JSON files.

This script processes the JSON files generated by the scrapers and creates
a consolidated Parquet dataset for analysis.
"""

import sys

import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import argparse
import concurrent.futures
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('build-local-dataset')

def get_json_files(data_dir, prefix="json"):
    """Get a list of all JSON files in the directory with the given prefix."""
    if not os.path.exists(data_dir):
        logger.error(f"Directory not found: {data_dir}")
        return []
    
    json_dir = os.path.join(data_dir, prefix)
    if not os.path.exists(json_dir):
        logger.error(f"JSON directory not found: {json_dir}")
        return []
    
    logger.info(f"Scanning for JSON files in {json_dir}...")
    files = []
    
    # Find all JSON files, excluding metadata files
    for file in os.listdir(json_dir):
        if file.endswith('.json') and not file.endswith('_meta.json'):
            files.append(os.path.join(json_dir, file))
    
    logger.info(f"Found {len(files)} JSON files")
    return files

def process_json_file(file_path):
    """Process a single JSON file into a pandas DataFrame."""
    try:
        # Get the file name (date part)
        file_name = os.path.basename(file_path)
        date_str = file_name.replace('.json', '')
        
        # Read the JSON file
        with open(file_path, 'r') as f:
            file_content = f.read()
        
        # Try to parse the JSON
        try:
            data = json.loads(file_content)
        except json.JSONDecodeError:
            logger.error(f"Error parsing JSON in {file_path}")
            return None
        
        # Check if we have games data
        if not data.get('games_found', False) or not data.get('games', []):
            logger.warning(f"No games found in {file_path}")
            return pd.DataFrame()
        
        # Extract games into a DataFrame
        games = data.get('games', [])
        if not games:
            logger.warning(f"Empty games list in {file_path}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(games)
        
        # Add date information
        df['source_date'] = date_str
        df['year'] = int(date_str.split('-')[0]) if '-' in date_str else None
        df['month'] = int(date_str.split('-')[1]) if '-' in date_str else None
        df['day'] = int(date_str.split('-')[2]) if '-' in date_str else None
        
        # Add processing metadata
        df['processed_at'] = datetime.now().isoformat()
        df['source_file'] = file_path
        
        return df
    
    except Exception as e:
        logger.error(f"Error processing {file_path}: {str(e)}")
        return None

def process_batch(file_paths):
    """Process a batch of JSON files and return a combined DataFrame."""
    dfs = []
    for file_path in file_paths:
        df = process_json_file(file_path)
        if df is not None and not df.empty:
            # Filter out empty columns to avoid FutureWarning
            df = df.dropna(axis=1, how='all')
            dfs.append(df)
    
    if dfs:
        # Only concatenate non-empty dataframes
        non_empty_dfs = [df for df in dfs if not df.empty]
        if non_empty_dfs:
            return pd.concat(non_empty_dfs, ignore_index=True)
    return pd.DataFrame()

def main():
    parser = argparse.ArgumentParser(description='Build Parquet dataset from local JSON files')
    parser.add_argument('--data-dir', default='output/data', help='Data directory containing JSON files')
    parser.add_argument('--output', default='output/dataset.parquet', help='Output parquet file')
    parser.add_argument('--workers', type=int, default=4, help='Number of worker threads')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for processing')
    parser.add_argument('--sample', type=int, default=0, help='Process only a sample of files (0 for all files)')
    args = parser.parse_args()
    
    # Make the data directory path absolute if it's not already
    if not os.path.isabs(args.data_dir):
        args.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.data_dir)
    
    # Make the output path absolute if it's not already
    if not os.path.isabs(args.output):
        args.output = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.output)
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    start_time = datetime.now()
    logger.info(f"Starting local dataset build at {start_time}")
    
    # Get all JSON files
    json_files = get_json_files(args.data_dir)
    
    if not json_files:
        logger.error("No JSON files found to process")
        return 1
    
    # Take a sample if requested
    if args.sample > 0:
        import random
        sample_size = min(args.sample, len(json_files))
        json_files = random.sample(json_files, sample_size)
        logger.info(f"Using a random sample of {sample_size} files")
    
    # Create batches
    batches = [json_files[i:i + args.batch_size] for i in range(0, len(json_files), args.batch_size)]
    logger.info(f"Processing {len(json_files)} files in {len(batches)} batches")
    
    # Process batches in parallel
    all_dfs = []
    total_records = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all batches
        future_to_batch = {executor.submit(process_batch, batch): i for i, batch in enumerate(batches)}
        
        # Process results as they complete
        for future in tqdm(concurrent.futures.as_completed(future_to_batch), total=len(batches)):
            batch_idx = future_to_batch[future]
            try:
                df = future.result()
                if df is not None and not df.empty:
                    records_in_batch = len(df)
                    total_records += records_in_batch
                    all_dfs.append(df)
                    logger.info(f"Batch {batch_idx+1}/{len(batches)} processed: {records_in_batch} records")
                else:
                    logger.warning(f"Batch {batch_idx+1}/{len(batches)} produced no data")
            except Exception as e:
                logger.error(f"Error processing batch {batch_idx+1}: {str(e)}")
    
    # Combine all dataframes
    if all_dfs:
        logger.info(f"Combining {len(all_dfs)} dataframes with {total_records} total records")
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        # Save as Parquet
        logger.info(f"Saving to {args.output}")
        pq.write_table(pa.Table.from_pandas(final_df), args.output)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Dataset build completed in {duration:.2f} seconds")
        logger.info(f"Total records: {len(final_df)}")
        logger.info(f"Dataset saved to {args.output}")
        
        # Show a sample of the data
        logger.info("\nSample data:")
        sample_rows = min(5, len(final_df))
        print(final_df.head(sample_rows))
        
        return 0
    else:
        logger.error("No data was processed. Check the JSON files and try again.")
        return 1

if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
"""
Build a consolidated Parquet dataset from all JSON files in S3.
This is a more efficient approach than using Step Functions and Lambda.
"""

import os
import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO
from datetime import datetime
import argparse
import concurrent.futures
from tqdm import tqdm

def get_json_files(bucket_name, prefix="data/json/"):
    """Get a list of all JSON files in the bucket with the given prefix."""
    s3 = boto3.client('s3')
    files = []
    
    print(f"Listing files in s3://{bucket_name}/{prefix}...")
    paginator = s3.get_paginator('list_objects_v2')
    
    # Exclude metadata files
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.json') and not key.endswith('_meta.json'):
                    files.append(key)
    
    print(f"Found {len(files)} JSON files")
    return files

def process_json_file(bucket_name, file_key):
    """Process a single JSON file into a pandas DataFrame."""
    s3 = boto3.client('s3')
    
    try:
        # Get the JSON file
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Try to parse the JSON
        try:
            data = json.loads(file_content)
        except json.JSONDecodeError:
            print(f"Error parsing JSON in {file_key}")
            return None
        
        # Extract date from filename
        date_str = os.path.basename(file_key).replace('.json', '')
        
        # Handle different JSON formats
        if isinstance(data, list):
            # If it's a list, assume each item is a game
            for item in data:
                if isinstance(item, dict):  # Ensure item is a dict before adding date
                    item['date'] = date_str
            # Convert to DataFrame and handle empty data
            df = pd.DataFrame(data) if data else pd.DataFrame()
            return df
        elif isinstance(data, dict) and 'games' in data and isinstance(data['games'], list):
            # If it has a 'games' key, use that
            for item in data['games']:
                if isinstance(item, dict):  # Ensure item is a dict before adding date
                    item['date'] = date_str
            # Convert to DataFrame and handle empty data
            df = pd.DataFrame(data['games']) if data['games'] else pd.DataFrame()
            return df
        elif isinstance(data, dict):
            # Single game record
            data['date'] = date_str
            return pd.DataFrame([data])
        else:
            print(f"Unknown JSON format in {file_key}")
            return None
    except Exception as e:
        print(f"Error processing {file_key}: {str(e)}")
        return None

def process_batch(bucket_name, file_keys):
    """Process a batch of JSON files and return a combined DataFrame."""
    dfs = []
    for file_key in file_keys:
        df = process_json_file(bucket_name, file_key)
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
    parser = argparse.ArgumentParser(description='Build Parquet dataset from JSON files in S3')
    parser.add_argument('--bucket', default='ncsh-app-data', help='S3 bucket name')
    parser.add_argument('--prefix', default='data/json/', help='S3 prefix for JSON files')
    parser.add_argument('--output', default='final_dataset.parquet', help='Output parquet file')
    parser.add_argument('--workers', type=int, default=4, help='Number of worker threads')
    parser.add_argument('--batch-size', type=int, default=25, help='Batch size for processing')
    parser.add_argument('--sample', type=int, default=0, help='Process only a sample of files (0 for all files)')
    parser.add_argument('--checkpoint', type=int, default=100, help='Save intermediate results after this many batches')
    args = parser.parse_args()
    
    start_time = datetime.now()
    print(f"Starting dataset build at {start_time}")
    
    # Get all JSON files
    json_files = get_json_files(args.bucket, args.prefix)
    
    # Take a sample if requested
    if args.sample > 0:
        import random
        sample_size = min(args.sample, len(json_files))
        json_files = random.sample(json_files, sample_size)
        print(f"Using a random sample of {sample_size} files")
    
    # Create batches
    batches = [json_files[i:i + args.batch_size] for i in range(0, len(json_files), args.batch_size)]
    print(f"Processing {len(json_files)} files in {len(batches)} batches")
    
    # Process batches in parallel with checkpointing
    all_dfs = []
    checkpoint_counter = 0
    total_records = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit batches in chunks to avoid overwhelming memory
        for batch_group_idx, batch_start_idx in enumerate(range(0, len(batches), args.checkpoint)):
            batch_group = batches[batch_start_idx:batch_start_idx + args.checkpoint]
            print(f"Processing batch group {batch_group_idx+1}/{(len(batches) + args.checkpoint - 1) // args.checkpoint} ({len(batch_group)} batches)")
            
            # Submit this group of batches
            future_to_batch = {
                executor.submit(process_batch, args.bucket, batch): i 
                for i, batch in enumerate(batch_group)
            }
            
            # Process results as they complete
            group_dfs = []
            for future in tqdm(concurrent.futures.as_completed(future_to_batch), total=len(batch_group)):
                batch_idx = future_to_batch[future]
                try:
                    df = future.result()
                    if not df.empty:
                        group_dfs.append(df)
                        total_records += len(df)
                        print(f"Batch {batch_start_idx + batch_idx + 1}/{len(batches)} processed, got {len(df)} records (total: {total_records})")
                except Exception as exc:
                    print(f"Batch {batch_start_idx + batch_idx} generated an exception: {exc}")
            
            # Combine this group's results
            if group_dfs:
                print(f"Combining results from batch group {batch_group_idx+1}...")
                group_df = pd.concat(group_dfs, ignore_index=True)
                all_dfs.append(group_df)
                
                # Save checkpoint if needed
                checkpoint_name = f"checkpoint_{batch_group_idx}.parquet"
                group_df.to_parquet(checkpoint_name, index=False)
                print(f"Saved checkpoint with {len(group_df)} records to {checkpoint_name}")
                
            # Clear memory
            group_dfs = []
    
    # Combine all DataFrames
    if all_dfs:
        print("Combining all data...")
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Basic data cleaning
        print("Cleaning and standardizing data...")
        
        # Extract year from the date field if needed
        if 'date' in combined_df.columns:
            # Parse the date field and extract components if needed
            combined_df['full_date'] = pd.to_datetime(combined_df['date'], errors='coerce')
            
            # Add year, month, day fields if not present
            if 'year' not in combined_df.columns:
                combined_df['year'] = combined_df['full_date'].dt.year
            if 'month' not in combined_df.columns:
                combined_df['month'] = combined_df['full_date'].dt.month
            if 'day' not in combined_df.columns:
                combined_df['day'] = combined_df['full_date'].dt.day
        
        # Convert numeric fields
        for col in combined_df.columns:
            if col in ['year', 'month', 'day', 'home_score', 'away_score']:
                combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
        
        # Save to Parquet
        print(f"Saving dataset with {len(combined_df)} records to {args.output}")
        combined_df.to_parquet(args.output, index=False)
        
        # Also save a CSV for easy viewing
        combined_df.to_csv(args.output.replace('.parquet', '.csv'), index=False)
        
        # Print summary
        print("\nDataset Summary:")
        print(f"Total records: {len(combined_df)}")
        
        if 'full_date' in combined_df.columns:
            print(f"Date range: {combined_df['full_date'].min()} to {combined_df['full_date'].max()}")
        
        if 'year' in combined_df.columns:
            print(f"Years covered: {combined_df['year'].min()} to {combined_df['year'].max()}")
            print(f"Records by year:")
            year_counts = combined_df['year'].value_counts().sort_index()
            for year, count in year_counts.items():
                print(f"  {year}: {count} games")
        
        # Upload to S3
        print(f"\nUploading {args.output} to S3...")
        s3 = boto3.client('s3')
        s3.upload_file(args.output, args.bucket, f"data/parquet/{args.output}")
        print(f"Uploaded to s3://{args.bucket}/data/parquet/{args.output}")
        
    else:
        print("No data was processed.")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"Finished in {duration:.2f} seconds")

if __name__ == '__main__':
    main()
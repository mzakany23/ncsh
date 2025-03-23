import os
import sys
import json
import logging
import boto3
import pandas as pd
import pyarrow as pa
import io
from datetime import datetime, timedelta, timezone
from models import GameData, Game
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def validate_and_transform_data(raw_data: List[Dict[Any, Any]]) -> List[Dict[str, Any]]:
    """Validate and transform raw data using Pydantic models with strict validation"""
    validated_data = []

    for record in raw_data:
        try:
            # Check if we have the alternative format (with league_name, game_date, etc.)
            if 'league_name' in record and 'game_date' in record:
                try:
                    # Parse the score field if it exists (format like "7 - 2")
                    home_score = None
                    away_score = None
                    if 'score' in record and record['score'] and ' - ' in record['score']:
                        try:
                            score_parts = record['score'].split(' - ')
                            if len(score_parts) == 2:
                                home_score = int(score_parts[0].strip())
                                away_score = int(score_parts[1].strip())
                        except (ValueError, IndexError):
                            logger.warning(f"Could not parse score: {record.get('score')}")

                    # Create Game object with field mapping
                    game = Game(
                        home_team=record.get('home_team', ''),
                        away_team=record.get('away_team', ''),
                        home_score=home_score,
                        away_score=away_score,
                        league=record.get('league_name', ''),
                        time=record.get('game_time')
                    )

                    # Create GameData instance
                    game_data = GameData(
                        date=record['game_date'],
                        games=game,
                        url=record.get('url'),
                        type=record.get('game_type'),
                        status=record.get('status'),
                        headers=record.get('headers'),
                        timestamp=record.get('timestamp', datetime.now(timezone.utc))
                    )

                    # Convert to flat dictionary structure
                    validated_data.append(game_data.to_dict())
                except Exception as e:
                    logger.warning(f"Invalid alternative format game data: {str(e)}")
                    continue
            else:
                # Handle case where games might be a list
                games = record.get('games', [])
                if not isinstance(games, list):
                    games = [games]

                # Process each game in the record
                for game in games:
                    if game is not None:
                        try:
                            # Create GameData instance with strict validation
                            game_data = GameData(
                                date=record['date'],
                                games=Game(**game),
                                url=record.get('url'),
                                type=record.get('type'),
                                status=record.get('status'),
                                headers=record.get('headers'),
                                timestamp=record.get('timestamp', datetime.now(timezone.utc))
                            )
                            # Convert to flat dictionary structure
                            validated_data.append(game_data.to_dict())
                        except Exception as e:
                            logger.warning(f"Invalid game data: {str(e)}")
                            continue

        except Exception as e:
            logger.warning(f"Invalid record: {str(e)}")
            continue

    return validated_data

def get_existing_dataset(bucket: str, key: str) -> pd.DataFrame:
    """Get the existing dataset from S3 if it exists, otherwise return an empty DataFrame"""
    s3 = boto3.client("s3")
    try:
        logger.info(f"Attempting to read existing dataset from s3://{bucket}/{key}")
        obj_response = s3.get_object(Bucket=bucket, Key=key)
        buffer = io.BytesIO(obj_response['Body'].read())

        # Read the existing Parquet file
        df = pd.read_parquet(buffer)
        logger.info(f"Successfully read existing dataset with {len(df)} rows")
        return df
    except s3.exceptions.NoSuchKey:
        logger.info(f"No existing dataset found at s3://{bucket}/{key}, starting with empty dataset")
        return pd.DataFrame()
    except Exception as e:
        logger.warning(f"Error reading existing dataset: {str(e)}, starting with empty dataset")
        return pd.DataFrame()

def get_last_processed_timestamp(bucket: str, prefix: str) -> Optional[datetime]:
    """
    Get the timestamp of the last successful processing run
    Uses a marker file in S3 to track when processing was last completed
    """
    s3 = boto3.client("s3")
    marker_key = f"{prefix.rstrip('/')}/last_processed.json"

    try:
        logger.info(f"Checking for last processed timestamp at s3://{bucket}/{marker_key}")
        response = s3.get_object(Bucket=bucket, Key=marker_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        timestamp_str = data.get('timestamp')

        if timestamp_str:
            # Parse with proper timezone handling
            if 'Z' in timestamp_str:
                last_processed = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                last_processed = datetime.fromisoformat(timestamp_str)

            # Ensure timezone-aware
            if last_processed.tzinfo is None:
                last_processed = last_processed.replace(tzinfo=timezone.utc)

            logger.info(f"Last processing run was at {last_processed}")
            return last_processed

    except s3.exceptions.NoSuchKey:
        logger.info(f"No last processed timestamp found at s3://{bucket}/{marker_key}")
    except Exception as e:
        logger.warning(f"Error getting last processed timestamp: {str(e)}")

    # If no marker file or errors, default to process only files from the last 2 days
    # This is a safety measure to avoid reprocessing the entire dataset
    default_time = datetime.now(timezone.utc) - timedelta(days=2)
    logger.info(f"Using default last processed time: {default_time}")
    return default_time

def update_last_processed_timestamp(bucket: str, prefix: str) -> None:
    """
    Update the timestamp of the last successful processing run
    Creates or updates a marker file in S3
    """
    s3 = boto3.client("s3")
    marker_key = f"{prefix.rstrip('/')}/last_processed.json"

    now = datetime.now(timezone.utc)
    data = {
        'timestamp': now.isoformat(),
        'status': 'success'
    }

    try:
        logger.info(f"Updating last processed timestamp to {now}")
        s3.put_object(
            Bucket=bucket,
            Key=marker_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        logger.info(f"Successfully updated last processed timestamp")
    except Exception as e:
        logger.warning(f"Error updating last processed timestamp: {str(e)}")

def convert_to_parquet(src_bucket, files, dst_bucket, dst_prefix, version: Optional[str] = None):
    """Convert JSON files to Parquet format and append to existing dataset

    Args:
        src_bucket: Source S3 bucket containing JSON files
        files: List of file keys to process
        dst_bucket: Destination S3 bucket for Parquet files
        dst_prefix: Prefix for Parquet files in the destination bucket
        version: Optional version identifier for the dataset (default: current timestamp)

    Returns:
        Dictionary with operation results
    """
    logger.info(f"Converting {len(files)} JSON files to Parquet")
    s3 = boto3.client("s3")

    # Use provided version or generate a timestamp
    if not version:
        version = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")

    logger.info(f'Using version identifier: {version}')

    # Add version to the prefix for better organization
    versioned_prefix = f"{dst_prefix}{version}/"

    try:
        # Process each JSON file
        all_validated_data = []
        validation_errors = []

        for key in files:
            logger.info(f"Processing {key}")
            try:
                # Read JSON file from S3
                obj_response = s3.get_object(Bucket=src_bucket, Key=key)
                data = obj_response['Body'].read()

                # Try reading as JSON Lines first
                try:
                    raw_data = pd.read_json(io.BytesIO(data), lines=True).to_dict('records')
                except ValueError:
                    # Fallback to standard JSON array
                    raw_data = pd.read_json(io.BytesIO(data)).to_dict('records')

                # Validate and transform the data
                validated_data = validate_and_transform_data(raw_data)
                all_validated_data.extend(validated_data)
                logger.info(f"Successfully processed {key}, valid records: {len(validated_data)}")

            except Exception as e:
                error_msg = f"Error processing {key}: {str(e)}"
                logger.error(error_msg)
                validation_errors.append(error_msg)
                continue

        if not all_validated_data:
            logger.warning("No valid records were processed")
            return {
                "status": "WARNING",
                "message": "No valid records were processed",
                "validation_errors": validation_errors
            }

        # Convert new data to DataFrame
        logger.info("Creating DataFrame from new data")
        new_df = pd.DataFrame(all_validated_data)
        logger.info(f"New data DataFrame shape: {new_df.shape}")

        # Get existing dataset
        current_key = f"{dst_prefix}data.parquet"
        existing_df = get_existing_dataset(dst_bucket, current_key)

        # Define PyArrow schema with updated timestamp handling
        schema = pa.schema([
            ('date', pa.timestamp('ns')),  # Make timestamp nullable
            ('home_team', pa.string()),
            ('away_team', pa.string()),
            ('home_score', pa.int64()),
            ('away_score', pa.int64()),
            ('league', pa.string()),
            ('time', pa.string()),
            ('url', pa.string()),
            ('type', pa.string()),
            ('status', pa.float64()),
            ('headers', pa.string()),
            ('timestamp', pa.timestamp('ns'))
        ])

        # If we have new data, combine with existing
        if not new_df.empty:
            if not existing_df.empty:
                # Create composite key for deduplication
                logger.info("Combining existing data with new data and deduplicating")
                existing_df['composite_key'] = existing_df.apply(
                    lambda x: f"{x['date']}_{x['home_team']}_{x['away_team']}_{x['league']}", axis=1
                )
                new_df['composite_key'] = new_df.apply(
                    lambda x: f"{x['date']}_{x['home_team']}_{x['away_team']}_{x['league']}", axis=1
                )

                # Combine datasets, keeping the most recent version of each record
                combined_df = pd.concat([existing_df, new_df])
                combined_df = combined_df.sort_values('timestamp', ascending=False)
                combined_df = combined_df.drop_duplicates(subset='composite_key', keep='first')
                combined_df = combined_df.drop(columns=['composite_key'])
                logger.info(f"Combined DataFrame shape after deduplication: {combined_df.shape}")
            else:
                logger.info("No existing data, using only new data")
                combined_df = new_df
        else:
            logger.info("No new data to add, using existing data")
            combined_df = existing_df

        if combined_df.empty:
            logger.warning("No data to write")
            return {
                "status": "WARNING",
                "message": "No data to write",
                "validation_errors": validation_errors
            }

        # Backup the existing file if it exists
        try:
            backup_key = f"{dst_prefix}data.backup.parquet"
            s3.head_object(Bucket=dst_bucket, Key=current_key)
            logger.info("Creating backup of existing Parquet file")
            s3.copy_object(
                Bucket=dst_bucket,
                CopySource={'Bucket': dst_bucket, 'Key': current_key},
                Key=backup_key
            )
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise
            logger.info("No existing Parquet file to backup")

        # Ensure date column is in datetime format with proper timezone handling
        try:
            if 'date' in combined_df.columns:
                logger.info("Converting date column to datetime format with consistent timezone")
                combined_df['date'] = pd.to_datetime(combined_df['date'], errors='coerce')

                # Make sure all datetime fields have the same timezone handling (or no timezone)
                combined_df['date'] = combined_df['date'].dt.tz_localize(None)

            if 'timestamp' in combined_df.columns:
                logger.info("Ensuring timestamp column has consistent timezone")
                combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], errors='coerce')
                combined_df['timestamp'] = combined_df['timestamp'].dt.tz_localize(None)

        except Exception as e:
            logger.warning(f"Error standardizing timezone info: {str(e)}. Will try to proceed.")

        # Write the combined data with explicit timezone handling
        out_buffer = io.BytesIO()
        logger.info("Converting DataFrame to Parquet format")

        try:
            combined_df.to_parquet(
                out_buffer,
                index=False,
                schema=schema
            )
        except Exception as e:
            logger.error(f"Error in to_parquet conversion: {str(e)}")

            # Try alternative approach without schema if needed
            logger.info("Trying alternative Parquet conversion approach")
            out_buffer = io.BytesIO()
            combined_df.to_parquet(
                out_buffer,
                index=False,
                engine='pyarrow'
            )

        out_buffer.seek(0)

        # Upload combined Parquet file (versioned)
        versioned_key = f"{versioned_prefix}data.parquet"
        logger.info(f"Uploading combined Parquet file ({len(combined_df)} rows) to s3://{dst_bucket}/{versioned_key}")
        s3.put_object(
            Bucket=dst_bucket,
            Key=versioned_key,
            Body=out_buffer.getvalue()
        )

        # Also upload to the standard path for backward compatibility
        logger.info(f"Also uploading to standard path: s3://{dst_bucket}/{current_key}")
        s3.put_object(
            Bucket=dst_bucket,
            Key=current_key,
            Body=out_buffer.getvalue()
        )

        # Update the last processed timestamp
        update_last_processed_timestamp(dst_bucket, dst_prefix)

        return {
            "status": "SUCCESS",
            "source": f"s3://{src_bucket}",
            "destination": f"s3://{dst_bucket}/{versioned_key}",
            "standardPath": f"s3://{dst_bucket}/{current_key}",
            "new_rows_processed": len(new_df),
            "total_rows": len(combined_df),
            "validation_errors": validation_errors if validation_errors else None,
            "version": version
        }

    except Exception as e:
        error_msg = f"Error converting to Parquet: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def list_json_files(bucket: str, prefix: str, only_recent: bool = True) -> List[str]:
    """
    List JSON files in the specified S3 bucket and prefix,
    excluding metadata files

    If only_recent is True, only returns files modified since the last processing run
    """
    logger.info(f"Listing JSON files in s3://{bucket}/{prefix}")
    s3 = boto3.client("s3")
    files = []

    # Get the timestamp of the last processing run
    last_processed = get_last_processed_timestamp(bucket, prefix.replace('json', 'parquet')) if only_recent else None

    if last_processed:
        logger.info(f"Filtering for files modified after {last_processed}")

    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    last_modified = obj['LastModified']

                    # Skip files that haven't been modified since the last processing run
                    if only_recent and last_processed:
                        # Ensure both are timezone-naive for proper comparison
                        naive_last_modified = last_modified.replace(tzinfo=None)
                        naive_last_processed = last_processed.replace(tzinfo=None) if last_processed.tzinfo else last_processed
                        if naive_last_modified <= naive_last_processed:
                            continue

                    # Filter out meta.json files
                    if key.endswith('meta.json'):
                        logger.info(f"Skipping metadata file: {key}")
                        continue

                    if key.endswith('.json') or key.endswith('.jsonl'):
                        files.append(key)
                        logger.info(f"Found file: {key}, Last Modified: {last_modified}")

        logger.info(f"Found {len(files)} JSON files to process")
        return files

    except Exception as e:
        error_msg = f"Error listing JSON files: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def build_dataset(src_bucket: str, src_prefix: str, dst_bucket: str, dst_prefix: str, version: Optional[str] = None) -> Dict[str, Any]:
    """Build or update the final dataset from all processed Parquet files

    Args:
        src_bucket: Source S3 bucket containing Parquet files
        src_prefix: Prefix for Parquet files in the source bucket
        dst_bucket: Destination S3 bucket for the dataset
        dst_prefix: Prefix for the dataset in the destination bucket
        version: Optional version identifier for the dataset (default: current timestamp)

    Returns:
        Dictionary with operation results
    """
    logger.info(f'Building final dataset from {src_bucket}/{src_prefix} to {dst_bucket}/{dst_prefix}')

    # Use provided version or generate a timestamp
    if not version:
        version = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")

    logger.info(f'Using version identifier: {version}')

    s3_client = boto3.client('s3')

    try:
        # List all Parquet files in the source prefix
        all_files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=src_bucket, Prefix=src_prefix)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.parquet'):
                        all_files.append(key)

        if not all_files:
            logger.warning(f'No Parquet files found in {src_bucket}/{src_prefix}')
            return {
                'status': 'SUCCESS',
                'message': 'No Parquet files found to build dataset',
                'filesProcessed': 0
            }

        logger.info(f'Found {len(all_files)} Parquet files to process')

        # Load and combine all Parquet files
        combined_df = None

        for file_key in all_files:
            logger.info(f'Processing file: {file_key}')
            try:
                response = s3_client.get_object(Bucket=src_bucket, Key=file_key)
                file_df = pd.read_parquet(io.BytesIO(response['Body'].read()))

                if combined_df is None:
                    combined_df = file_df
                else:
                    combined_df = pd.concat([combined_df, file_df], ignore_index=True)
            except Exception as e:
                logger.error(f'Error processing file {file_key}: {str(e)}')
                # Continue processing other files
                continue

        if combined_df is None or combined_df.empty:
            logger.warning('No valid data found in any Parquet files')
            return {
                'status': 'SUCCESS',
                'message': 'No valid data found in Parquet files',
                'filesProcessed': 0
            }

        # Remove duplicates and sort
        logger.info(f'Raw dataset size before deduplication: {len(combined_df)}')

        # Use a combination of fields that should be unique for each game
        combined_df.drop_duplicates(subset=['date', 'field', 'home_team', 'away_team', 'time'], inplace=True)

        # Ensure datetime columns have consistent timezone handling
        try:
            logger.info("Standardizing datetime columns for consistent Parquet handling")

            # Handle date column
            if 'date' in combined_df.columns:
                logger.info("Ensuring date column is properly formatted")
                combined_df['date'] = pd.to_datetime(combined_df['date'], errors='coerce')
                # Make timezone-naive for Parquet compatibility
                if hasattr(combined_df['date'], 'dt'):
                    combined_df['date'] = combined_df['date'].dt.tz_localize(None)

            # Handle timestamp column
            if 'timestamp' in combined_df.columns:
                logger.info("Ensuring timestamp column is properly formatted")
                combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], errors='coerce')
                # Make timezone-naive for Parquet compatibility
                if hasattr(combined_df['timestamp'], 'dt'):
                    combined_df['timestamp'] = combined_df['timestamp'].dt.tz_localize(None)

            # Now sort by date and time
            logger.info("Sorting by date and time")
            combined_df.sort_values(by=['date', 'time'], inplace=True, na_position='last')

        except Exception as e:
            logger.error(f"Error handling datetime columns: {str(e)}")
            logger.info("Attempting alternative approach for timestamp handling")

            try:
                # If direct conversion fails, try an alternate approach
                # First convert any problematic columns to string
                if 'timestamp' in combined_df.columns:
                    # Check the column type and try different approaches based on that
                    col_type = combined_df['timestamp'].dtype
                    logger.info(f"Timestamp column data type: {col_type}")

                    if pd.api.types.is_datetime64_any_dtype(col_type):
                        logger.info("Converting timestamp from datetime to string")
                        combined_df['timestamp'] = combined_df['timestamp'].astype(str)
                    else:
                        logger.info("Attempting to parse timestamp strings")
                        # Parse strings to datetime then back to strings to ensure consistency
                        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], errors='coerce').astype(str)

                # Also handle the date column if it exists
                if 'date' in combined_df.columns:
                    logger.info("Creating a datetime_sort column for sorting purposes")
                    # Create a temporary column for sorting
                    combined_df['datetime_sort'] = pd.to_datetime(combined_df['date'], errors='coerce')
                    # Sort by the new column and time
                    combined_df.sort_values(by=['datetime_sort', 'time'], inplace=True, na_position='last')
                    # Drop the temporary sorting column
                    combined_df.drop(columns=['datetime_sort'], inplace=True)

                logger.info("Alternative timestamp handling approach applied")
            except Exception as alt_err:
                logger.error(f"Error in alternative timestamp handling approach: {str(alt_err)}")
                logger.info("Proceeding with original data without timestamp modifications")

        logger.info(f'Final dataset size after deduplication: {len(combined_df)}')

        # Define schema with consistent timestamp handling
        try:
            logger.info("Setting up PyArrow schema for consistent Parquet conversion")
            parquet_schema = pa.schema([
                ('date', pa.timestamp('ns')),
                ('home_team', pa.string()),
                ('away_team', pa.string()),
                ('home_score', pa.int64()),
                ('away_score', pa.int64()),
                ('league', pa.string()),
                ('time', pa.string()),
                ('url', pa.string()),
                ('type', pa.string()),
                ('status', pa.float64()),
                ('headers', pa.string()),
                ('timestamp', pa.string())  # Use string type for timestamp to avoid PyArrow issues
            ])
        except Exception as schema_err:
            logger.error(f"Error creating schema: {str(schema_err)}")
            logger.info("Will proceed without explicit schema")
            parquet_schema = None

        # Save as both Parquet and CSV
        try:
            logger.info("Converting to Parquet format")
            parquet_buffer = io.BytesIO()

            if parquet_schema:
                # Try with schema first
                try:
                    combined_df.to_parquet(parquet_buffer, schema=parquet_schema, index=False)
                except Exception as e:
                    logger.warning(f"Parquet conversion with schema failed: {str(e)}")
                    logger.info("Trying without schema")
                    parquet_buffer = io.BytesIO()
                    combined_df.to_parquet(parquet_buffer, index=False)
            else:
                # No schema available
                combined_df.to_parquet(parquet_buffer, index=False)

            parquet_buffer.seek(0)
        except Exception as parquet_err:
            logger.error(f"Error in Parquet conversion: {str(parquet_err)}")
            logger.info("Attempting simpler conversion approach")

            try:
                # If direct Parquet conversion fails, try JSON serialization as intermediate step
                json_str = combined_df.to_json(orient='records', date_format='iso')
                clean_df = pd.read_json(json_str, orient='records')

                parquet_buffer = io.BytesIO()
                clean_df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                logger.info("Successfully converted to Parquet using alternative approach")
            except Exception as alt_parquet_err:
                logger.error(f"Alternative Parquet conversion also failed: {str(alt_parquet_err)}")
                return {
                    'status': 'ERROR',
                    'message': f'Failed to convert data to Parquet format: {str(alt_parquet_err)}',
                    'filesProcessed': len(all_files)
                }

        # Convert to CSV (generally more robust)
        try:
            logger.info("Converting to CSV format")
            csv_buffer = io.StringIO()
            combined_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
        except Exception as csv_err:
            logger.error(f"Error in CSV conversion: {str(csv_err)}")
            return {
                'status': 'ERROR',
                'message': f'Failed to convert data to CSV format: {str(csv_err)}',
                'filesProcessed': len(all_files)
            }

        # Upload the final datasets with version in filename
        parquet_key = f"{dst_prefix}ncsoccer_games_{version}.parquet"
        csv_key = f"{dst_prefix}ncsoccer_games_{version}.csv"

        # Also create 'latest' versions for easy access
        parquet_latest_key = f"{dst_prefix}ncsoccer_games_latest.parquet"
        csv_latest_key = f"{dst_prefix}ncsoccer_games_latest.csv"

        # Upload versioned datasets
        s3_client.put_object(
            Body=parquet_buffer.getvalue(),
            Bucket=dst_bucket,
            Key=parquet_key
        )

        s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=dst_bucket,
            Key=csv_key
        )

        # Upload 'latest' versions
        s3_client.put_object(
            Body=parquet_buffer.getvalue(),
            Bucket=dst_bucket,
            Key=parquet_latest_key
        )

        s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=dst_bucket,
            Key=csv_latest_key
        )

        logger.info(f'Successfully built and uploaded final dataset:')
        logger.info(f' - Versioned files: {dst_bucket}/{parquet_key} and {dst_bucket}/{csv_key}')
        logger.info(f' - Latest files: {dst_bucket}/{parquet_latest_key} and {dst_bucket}/{csv_latest_key}')

        return {
            'status': 'SUCCESS',
            'message': 'Successfully built and uploaded final dataset',
            'filesProcessed': len(all_files),
            'totalRecords': len(combined_df),
            'parquetPath': f"s3://{dst_bucket}/{parquet_key}",
            'csvPath': f"s3://{dst_bucket}/{csv_key}",
            'latestParquetPath': f"s3://{dst_bucket}/{parquet_latest_key}",
            'latestCsvPath': f"s3://{dst_bucket}/{csv_latest_key}",
            'version': version
        }

    except Exception as e:
        error_msg = f'Error building final dataset: {str(e)}'
        logger.error(error_msg)
        raise Exception(error_msg)

def check_backfill_status(src_bucket: str, src_prefix: str) -> Dict[str, Any]:
    """Check the status of a backfill operation by examining markers in S3"""
    logger.info(f'Checking backfill status in {src_bucket}/{src_prefix}')

    s3_client = boto3.client('s3')

    try:
        # Check for backfill marker files
        backfill_in_progress = False
        backfill_completed = False

        # Check for in-progress marker
        try:
            s3_client.head_object(Bucket=src_bucket, Key=f"{src_prefix}backfill_in_progress.marker")
            backfill_in_progress = True
        except s3_client.exceptions.ClientError:
            # Marker doesn't exist, which is fine
            pass

        # Check for completed marker
        try:
            s3_client.head_object(Bucket=src_bucket, Key=f"{src_prefix}backfill_completed.marker")
            backfill_completed = True
        except s3_client.exceptions.ClientError:
            # Marker doesn't exist, which is fine
            pass

        # Count the number of files processed
        file_count = 0
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=src_bucket, Prefix=src_prefix)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.json') or key.endswith('.jsonl'):
                        file_count += 1

        logger.info(f'Found {file_count} JSON files in {src_bucket}/{src_prefix}')

        # Determine the backfill status
        status = "UNKNOWN"
        if backfill_completed:
            status = "COMPLETED"
        elif backfill_in_progress:
            status = "IN_PROGRESS"
        else:
            # If no markers exist but we have files, likely a completed backfill
            if file_count > 0:
                status = "COMPLETED"
            else:
                status = "NOT_STARTED"

        return {
            'status': status,
            'filesCount': file_count,
            'inProgressMarker': backfill_in_progress,
            'completedMarker': backfill_completed
        }

    except Exception as e:
        error_msg = f'Error checking backfill status: {str(e)}'
        logger.error(error_msg)
        raise Exception(error_msg)

def process_all(src_bucket: str, src_prefix: str, dst_bucket: str, dst_prefix: str) -> Dict[str, Any]:
    """Process all JSON files regardless of their last modified date"""
    logger.info(f'Processing all JSON files from {src_bucket}/{src_prefix} to {dst_bucket}/{dst_prefix}')

    try:
        # List all files without time filtering
        files = list_json_files(src_bucket, src_prefix, only_recent=False)

        if not files:
            logger.info("No JSON files found to process")
            return {
                "status": "SUCCESS",
                "message": "No files to process",
                "filesProcessed": 0
            }

        # Convert all files to Parquet
        result = convert_to_parquet(src_bucket, files, dst_bucket, dst_prefix)

        return {
            "status": "SUCCESS",
            "message": f"Successfully processed all {len(files)} files",
            "filesProcessed": len(files),
            "newRowsProcessed": result.get("new_rows_processed", 0)
        }

    except Exception as e:
        error_msg = f'Error in process_all operation: {str(e)}'
        logger.error(error_msg)
        raise Exception(error_msg)

def lambda_handler(event, context):
    """AWS Lambda handler for the processing pipeline"""
    logger.info(f"Processing event: {json.dumps(event)}")

    try:
        # Get operation type
        operation = event.get('operation', 'convert')  # Default to convert for backward compatibility

        # Check if we should process all files or only recent ones
        force_full_reprocess = event.get('force_full_reprocess', False)

        # Get version for dataset versioning
        version = event.get('version')
        if version:
            logger.info(f"Using provided version identifier: {version}")

        # Get architecture version - default to v2 now
        architecture_version = event.get('architecture_version', 'v2')
        logger.info(f"Using architecture version: {architecture_version}")

        # Get environment variables with defaults
        src_bucket = event.get('src_bucket', os.environ.get("DATA_BUCKET", "ncsh-app-data"))
        dst_bucket = event.get('dst_bucket', os.environ.get("DATA_BUCKET", "ncsh-app-data"))

        # Set paths for v2 directory structure
        src_prefix = event.get('src_prefix', 'v2/processed/json/')
        dst_prefix = event.get('dst_prefix', 'v2/processed/parquet/')
        logger.info(f"Using directory structure: src={src_prefix}, dst={dst_prefix}")

        # For backward compatibility
        if architecture_version == 'v1':
            logger.warning("v1 architecture is deprecated, please update to use v2")
            # Still use v2 paths but log a warning

        if operation == "list_files":
            # List JSON files, optionally filtering for only recent ones
            files = list_json_files(src_bucket, src_prefix, not force_full_reprocess)
            return {
                "files": files,
                "filesProcessed": len(files),
                "src_bucket": src_bucket,
                "src_prefix": src_prefix,
                "dst_bucket": dst_bucket,
                "dst_prefix": dst_prefix,
                "force_full_reprocess": force_full_reprocess,
                "version": version,
                "architecture_version": architecture_version
            }

        elif operation == "convert":
            # Get files from previous step
            files = event.get('files', [])
            if not files:
                logger.info("No files provided for conversion")
                return {
                    "status": "SUCCESS",
                    "message": "No new files to process",
                    "new_rows_processed": 0
                }

            # Convert files to Parquet with versioning
            result = convert_to_parquet(src_bucket, files, dst_bucket, dst_prefix, version)
            return result

        elif operation == "build_dataset":
            # Build final dataset from all Parquet files with versioning
            return build_dataset(src_bucket, src_prefix, dst_bucket, dst_prefix, version)

        elif operation == "check_backfill_status":
            # Check status of backfill operation
            return check_backfill_status(src_bucket, src_prefix)

        elif operation == "process_all":
            # Process all files regardless of last modified time
            result = process_all(src_bucket, src_prefix, dst_bucket, dst_prefix)

            # If successful, also build a versioned dataset
            if result.get('status') == 'SUCCESS' and result.get('filesProcessed', 0) > 0:
                logger.info("Building versioned dataset after processing all files")
                dataset_result = build_dataset(src_bucket, src_prefix, dst_bucket, dst_prefix, version)
                result['datasetResult'] = dataset_result

            # Add architecture version to result
            result['architecture_version'] = architecture_version
            result['src_prefix'] = src_prefix
            result['dst_prefix'] = dst_prefix

            return result

        else:
            raise ValueError(f"Unknown operation: {operation}")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error in processing pipeline: {error_msg}")
        raise Exception(error_msg)

if __name__ == '__main__':
    # Handle command line execution for testing
    if len(sys.argv) != 2:
        print("Usage: python lambda_parquet_runction.py '<event_json>'")
        sys.exit(1)

    event = json.loads(sys.argv[1])
    response = lambda_handler(event, None)
    print(json.dumps(response, indent=2))
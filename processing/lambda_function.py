import os
import sys
import json
import logging
import boto3
import pandas as pd
import pyarrow as pa
import io
from datetime import datetime
from models import GameData, Game
from typing import List, Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def validate_and_transform_data(raw_data: List[Dict[Any, Any]]) -> List[Dict[str, Any]]:
    """Validate and transform raw data using Pydantic models with strict validation"""
    validated_data = []

    for record in raw_data:
        try:
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
                            timestamp=record.get('timestamp', datetime.utcnow())
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

def convert_to_parquet(src_bucket, files, dst_bucket, dst_prefix):
    """Convert JSON files to Parquet format with strict schema validation"""
    logger.info(f"Converting {len(files)} JSON files to Parquet")
    s3 = boto3.client("s3")

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

        # Convert to DataFrame
        logger.info("Creating DataFrame")
        df = pd.DataFrame(all_validated_data)
        logger.info(f"DataFrame shape: {df.shape}")

        # Convert to Parquet with schema enforcement
        logger.info("Converting to Parquet format")
        out_buffer = io.BytesIO()

        # Define PyArrow schema
        schema = pa.schema([
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
            ('timestamp', pa.timestamp('ns'))
        ])

        df.to_parquet(
            out_buffer,
            index=False,
            schema=schema
        )
        out_buffer.seek(0)

        # Define file locations
        current_key = f"{dst_prefix}data.parquet"
        backup_key = f"{dst_prefix}data.backup.parquet"

        try:
            # Create backup of existing file if it exists
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

        # Upload new Parquet file
        logger.info(f"Uploading Parquet file to s3://{dst_bucket}/{current_key}")
        s3.put_object(
            Bucket=dst_bucket,
            Key=current_key,
            Body=out_buffer.getvalue()
        )

        return {
            "status": "SUCCESS",
            "source": f"s3://{src_bucket}",
            "destination": f"s3://{dst_bucket}/{current_key}",
            "rows_processed": len(df),
            "validation_errors": validation_errors if validation_errors else None
        }

    except Exception as e:
        error_msg = f"Error converting to Parquet: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def list_json_files(bucket: str, prefix: str) -> List[str]:
    """List all JSON files in the specified S3 bucket and prefix"""
    logger.info(f"Listing JSON files in s3://{bucket}/{prefix}")
    s3 = boto3.client("s3")
    files = []

    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.json') or key.endswith('.jsonl'):
                        files.append(key)
                        logger.info(f"Found file: {key}")

        logger.info(f"Found {len(files)} JSON files")
        return files

    except Exception as e:
        error_msg = f"Error listing JSON files: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def lambda_handler(event, context):
    """AWS Lambda handler for the processing pipeline"""
    logger.info(f"Processing event: {json.dumps(event)}")

    try:
        # Get operation type
        operation = event.get('operation', 'convert')  # Default to convert for backward compatibility

        # Get environment variables with defaults
        src_bucket = event.get('src_bucket', os.environ.get("DATA_BUCKET", "ncsh-app-data"))
        src_prefix = event.get('src_prefix', os.environ.get("JSON_PREFIX", "data/json/"))
        dst_bucket = event.get('dst_bucket', os.environ.get("DATA_BUCKET", "ncsh-app-data"))
        dst_prefix = event.get('dst_prefix', os.environ.get("PARQUET_PREFIX", "data/parquet/"))

        if operation == "list_files":
            # List JSON files
            files = list_json_files(src_bucket, src_prefix)
            return {
                "files": files,
                "src_bucket": src_bucket,
                "src_prefix": src_prefix,
                "dst_bucket": dst_bucket,
                "dst_prefix": dst_prefix
            }

        elif operation == "convert":
            # Get files from previous step
            files = event.get('files', [])
            if not files:
                raise ValueError("No files provided for conversion")

            # Convert files to Parquet
            result = convert_to_parquet(src_bucket, files, dst_bucket, dst_prefix)
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
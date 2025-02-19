import os
import sys
import json
import logging
import boto3
import pandas as pd
import io
from datetime import datetime
from models import GameData, Game
from typing import List, Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def validate_and_transform_data(raw_data: List[Dict[Any, Any]]) -> List[Dict[str, Any]]:
    """Validate and transform raw data using Pydantic models"""
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
                        # Create GameData instance with a single Game
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
    """Convert JSON files to Parquet format"""
    logger.info(f"Converting {len(files)} JSON files to Parquet")
    s3 = boto3.client("s3")

    try:
        # Process each JSON file
        all_validated_data = []
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
                logger.error(f"Error processing {key}: {str(e)}")
                continue

        if not all_validated_data:
            logger.warning("No valid records were processed")
            return {
                "status": "WARNING",
                "message": "No valid records were processed"
            }

        # Convert to DataFrame
        logger.info("Creating DataFrame")
        df = pd.DataFrame(all_validated_data)
        logger.info(f"DataFrame shape: {df.shape}")

        # Convert to Parquet
        logger.info("Converting to Parquet format")
        out_buffer = io.BytesIO()
        df.to_parquet(
            out_buffer,
            index=False,
            # Specify schema for consistent column types
            schema={
                'date': 'timestamp[ns]',
                'home_team': 'string',
                'away_team': 'string',
                'home_score': 'int64',
                'away_score': 'int64',
                'league': 'string',
                'time': 'string',
                'url': 'string',
                'type': 'string',
                'status': 'float64',
                'headers': 'string',
                'timestamp': 'timestamp[ns]'
            }
        )
        out_buffer.seek(0)

        # Use a fixed location for the Parquet file
        dst_key = f"{dst_prefix}current/data.parquet"
        backup_key = f"{dst_prefix}backup/data.parquet"

        # Create backup of existing file if it exists
        try:
            s3.head_object(Bucket=dst_bucket, Key=dst_key)
            logger.info("Creating backup of existing Parquet file")
            s3.copy_object(
                Bucket=dst_bucket,
                CopySource={'Bucket': dst_bucket, 'Key': dst_key},
                Key=backup_key
            )
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise
            logger.info("No existing Parquet file to backup")

        # Clean up old versioned files
        try:
            logger.info("Cleaning up old versioned Parquet files")
            old_versions = s3.list_objects_v2(
                Bucket=dst_bucket,
                Prefix=f"{dst_prefix}v"
            )
            if 'Contents' in old_versions:
                for obj in old_versions['Contents']:
                    logger.info(f"Deleting old version: {obj['Key']}")
                    s3.delete_object(Bucket=dst_bucket, Key=obj['Key'])
        except Exception as e:
            logger.warning(f"Error cleaning up old versions: {str(e)}")

        # Upload new Parquet file
        logger.info(f"Uploading Parquet file to s3://{dst_bucket}/{dst_key}")
        s3.put_object(
            Bucket=dst_bucket,
            Key=dst_key,
            Body=out_buffer.getvalue()
        )

        return {
            "status": "SUCCESS",
            "source": f"s3://{src_bucket}",
            "destination": f"s3://{dst_bucket}/{dst_key}",
            "rows_processed": len(df)
        }

    except Exception as e:
        logger.error(f"Error converting to Parquet: {str(e)}")
        raise

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
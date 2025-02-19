import os
import sys
import json
import logging
import boto3
import pandas as pd
import io
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def list_json_files(bucket, prefix):
    """List all JSON files in the specified S3 location"""
    logger.info(f"Listing JSON files in s3://{bucket}/{prefix}")
    s3 = boto3.client("s3")

    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            logger.warning("No JSON files found")
            return []

        # Filter for .json and .jsonl files
        json_files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith(('.json', '.jsonl'))
        ]

        logger.info(f"Found {len(json_files)} JSON files")
        return json_files

    except Exception as e:
        logger.error(f"Error listing JSON files: {str(e)}")
        raise

def convert_to_parquet(src_bucket, files, dst_bucket, dst_prefix):
    """Convert JSON files to Parquet format"""
    logger.info(f"Converting {len(files)} JSON files to Parquet")
    s3 = boto3.client("s3")

    try:
        # Process each JSON file
        dataframes = []
        for key in files:
            logger.info(f"Processing {key}")
            try:
                # Read JSON file from S3
                obj_response = s3.get_object(Bucket=src_bucket, Key=key)
                data = obj_response['Body'].read()

                # Try reading as JSON Lines first
                try:
                    df = pd.read_json(io.BytesIO(data), lines=True)
                except ValueError:
                    # Fallback to standard JSON array
                    df = pd.read_json(io.BytesIO(data))

                dataframes.append(df)
                logger.info(f"Successfully processed {key}, shape: {df.shape}")

            except Exception as e:
                logger.error(f"Error processing {key}: {str(e)}")
                continue

        if not dataframes:
            logger.warning("No valid JSON files were processed")
            return {
                "status": "WARNING",
                "message": "No valid JSON files were processed"
            }

        # Combine all dataframes
        logger.info("Combining dataframes")
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Combined shape: {combined_df.shape}")

        # Convert to Parquet
        logger.info("Converting to Parquet format")
        out_buffer = io.BytesIO()
        combined_df.to_parquet(out_buffer, index=False)
        out_buffer.seek(0)

        # Create version string based on current UTC time
        version = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        dst_key = f"{dst_prefix}v{version}/data.parquet"

        # Upload Parquet file
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
            "version": version,
            "rows_processed": len(combined_df)
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
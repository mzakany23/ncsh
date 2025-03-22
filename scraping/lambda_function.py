import os
import sys
import json
import logging
import calendar
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from ncsoccer.runner import run_scraper, run_month, run_date_range
from ncsoccer.pipeline.config import DataArchitectureVersion

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Suppress DEBUG messages from third-party libraries
logging.getLogger("twisted").setLevel(logging.WARNING)
logging.getLogger("scrapy").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)

def lambda_handler(event, context):
    """
    AWS Lambda handler function

    Args:
        event (dict): Lambda event with scraping configuration:
            {
                "mode": "day|month|date_range",
                "parameters": {
                    "year": "2024",
                    "month": "01",
                    "day": "01",
                    "force_scrape": true,
                    "architecture_version": "v1", # Optional: "v1" or "v2"
                    "start_date": "2024-01-01",  # For date_range mode
                    "end_date": "2024-01-31",    # For date_range mode
                    "bucket_name": "ncsh-app-data", # Optional: S3 bucket name
                    "html_prefix": "data/html",  # Optional: HTML prefix in S3
                    "json_prefix": "data/json",  # Optional: JSON prefix in S3
                    "lookup_file": "data/lookup.json", # Optional: Lookup file path
                    "region": "us-east-2"        # Optional: AWS region
                }
            }
        context (LambdaContext): Lambda context

    Returns:
        dict: Result of the Lambda execution
    """

    try:
        logger.info(f"Received event: {json.dumps(event)}")

        # Extract mode
        mode = event.get('mode', 'day')
        logger.info(f"Operating in mode: {mode}")

        # Extract parameters with defaults
        parameters = event.get('parameters', {})

        # IMPORTANT: In Lambda, always force S3 storage, never use file storage
        storage_type = 's3'  # Always use S3 in Lambda
        bucket_name = parameters.get('bucket_name', 'ncsh-app-data')
        html_prefix = parameters.get('html_prefix', 'data/html')
        json_prefix = parameters.get('json_prefix', 'data/json')
        lookup_file = parameters.get('lookup_file', 'data/lookup.json')
        region = parameters.get('region', 'us-east-2')
        force_scrape = parameters.get('force_scrape', False)

        # Extract architecture version (defaults to v1 for backward compatibility)
        architecture_version = parameters.get('architecture_version', 'v1')
        logger.info(f"Using data architecture version: {architecture_version}")

        # Extract max_wait parameter if provided, otherwise use default value of 300 seconds
        max_wait = int(parameters.get('max_wait', 300))
        logger.info(f"Using max_wait value: {max_wait} seconds")

        # Log storage configuration
        logger.info(f"Storage configuration: type={storage_type}, bucket={bucket_name}, region={region}")
        logger.info(f"Path configuration: html={html_prefix}, json={json_prefix}, lookup={lookup_file}")

        # Create common parameters dictionary
        common_params = {
            'storage_type': storage_type,  # Always S3 in Lambda
            'bucket_name': bucket_name,
            'html_prefix': html_prefix,
            'json_prefix': json_prefix,
            'lookup_file': lookup_file,
            'lookup_type': 's3',  # Always use S3 for lookup in Lambda
            'region': region,
            'force_scrape': force_scrape,
            'architecture_version': architecture_version,
            'max_wait': max_wait
        }

        # Validate architecture version
        try:
            arch_version = DataArchitectureVersion(architecture_version.lower())
            logger.info(f"Architecture version validated: {arch_version.value}")
        except ValueError:
            logger.warning(f"Invalid architecture_version: {architecture_version}. Using v1 as default.")
            architecture_version = 'v1'
            common_params['architecture_version'] = 'v1'

        # Get current date for defaults
        now = datetime.now()

        # Handle different modes
        if mode == 'day':
            # Day mode - scrape a single day
            year = int(parameters.get('year', now.year))
            month = int(parameters.get('month', now.month))
            day = int(parameters.get('day', now.day))

            logger.info(f"Running in day mode for {year}-{month:02d}-{day:02d}")

            result = run_scraper(
                year=year,
                month=month,
                day=day,
                **common_params
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': result,
                    'date': f"{year}-{month:02d}-{day:02d}",
                    'storage_type': storage_type,
                    'bucket_name': bucket_name,
                    'architecture_version': architecture_version
                })
            }

        elif mode == 'month':
            # Month mode - scrape a whole month
            year = int(parameters.get('year', now.year))
            month = int(parameters.get('month', now.month))

            logger.info(f"Running in month mode for {year}-{month:02d}")

            result = run_month(
                year=year,
                month=month,
                **common_params
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': result,
                    'year': year,
                    'month': month,
                    'storage_type': storage_type,
                    'bucket_name': bucket_name,
                    'architecture_version': architecture_version
                })
            }

        elif mode == 'date_range':
            # Date range mode - scrape a range of dates
            start_date_str = parameters.get('start_date')
            end_date_str = parameters.get('end_date')

            if not start_date_str or not end_date_str:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'start_date and end_date are required for date_range mode'
                    })
                }

            logger.info(f"Running in date_range mode from {start_date_str} to {end_date_str}")

            # Parse dates
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            except ValueError as e:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': f'Invalid date format: {str(e)}. Use YYYY-MM-DD'
                    })
                }

            # Use the updated run_date_range function
            result = run_date_range(
                start_date=start_date,
                end_date=end_date,
                **common_params
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': result,
                    'start_date': start_date_str,
                    'end_date': end_date_str,
                    'storage_type': storage_type,
                    'bucket_name': bucket_name,
                    'architecture_version': architecture_version
                })
            }

        else:
            # Invalid mode
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Invalid mode: {mode}. Use day, month, or date_range'
                })
            }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

if __name__ == "__main__":
    # For local testing
    result = lambda_handler({
        'mode': 'day',
        'parameters': {
            'year': 2023,
            'month': 2,
            'day': 1,
            'force_scrape': True,
            'architecture_version': 'v2',
            'bucket_name': 'ncsh-app-data',
            'storage_type': 's3',
            'html_prefix': 'data/html',
            'json_prefix': 'data/json',
            'lookup_file': 'data/lookup.json',
            'region': 'us-east-2',
            'max_wait': 300
        }
    }, None)
    print(json.dumps(result, indent=2))

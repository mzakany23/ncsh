import os
import sys
import json
import time
import logging
import calendar
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from ncsoccer.runner import run_scraper, run_date_range
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
    AWS Lambda handler function for date range scraping

    Args:
        event (dict): Lambda event with scraping configuration:
            {
                # Legacy format (deprecated but supported for backward compatibility)
                "mode": "day|month|date_range",
                "parameters": {
                    "year": "2024",
                    "month": "01",
                    "day": "01",
                    "force_scrape": true,
                    "architecture_version": "v1",
                    "start_date": "2024-01-01",  # For date_range mode
                    "end_date": "2024-01-31",    # For date_range mode
                    ...
                }

                # OR

                # Unified format (recommended)
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "force_scrape": true,
                "architecture_version": "v1",
                "bucket_name": "ncsh-app-data"
            }
        context (LambdaContext): Lambda context

    Returns:
        dict: Result of the scraping operation
    """
    try:
        logger.info(f"Received event: {json.dumps(event)}")

        # Detect if the event is using the new unified format or legacy format
        if "start_date" in event and "end_date" in event:
            # Unified format
            return handle_unified_format(event, context)
        elif "mode" in event:
            # Legacy format with mode
            logger.warning("Using legacy format with 'mode' parameter. Consider migrating to the unified format.")
            return handle_legacy_format(event, context)
        else:
            error_msg = "Invalid event format. Must include either 'mode' parameter (legacy) or 'start_date'/'end_date' parameters (unified)."
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'error': error_msg
                })
            }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e)
            })
        }

def handle_unified_format(event, context):
    """
    Handle the unified format event with start_date and end_date

    Args:
        event (dict): Lambda event in unified format
        context (LambdaContext): Lambda context

    Returns:
        dict: Result of the scraping operation
    """
    try:
        # Get start and end dates
        start_date_str = event.get('start_date')
        end_date_str = event.get('end_date')

        if not start_date_str or not end_date_str:
            error_msg = "Missing required parameters: start_date and end_date"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'error': error_msg
                })
            }

        # Parse dates
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError as e:
            error_msg = f"Invalid date format: {str(e)}. Use YYYY-MM-DD format."
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'error': error_msg
                })
            }

        # Extract parameters
        force_scrape = event.get('force_scrape', False)
        architecture_version = event.get('architecture_version', 'v1')
        bucket_name = event.get('bucket_name', 'ncsh-app-data')
        html_prefix = event.get('html_prefix', 'data/html')
        json_prefix = event.get('json_prefix', 'data/json')
        lookup_file = event.get('lookup_file', 'data/lookup.json')
        region = event.get('region', 'us-east-2')
        max_wait = int(event.get('max_wait', 300))

        # Always use S3 in Lambda
        storage_type = 's3'

        # Common parameters
        common_params = {
            'storage_type': storage_type,
            'bucket_name': bucket_name,
            'html_prefix': html_prefix,
            'json_prefix': json_prefix,
            'lookup_file': lookup_file,
            'lookup_type': 's3',
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

        # Record start time for timeout tracking
        start_time = time.time()
        max_runtime = 240  # 240 seconds = 4 minutes (leaving buffer for 5 min Lambda)

        # Process each date in the range
        current = start_date
        success_count = 0
        failed_dates = []

        while current <= end_date:
            # Check if we're approaching the timeout
            elapsed = time.time() - start_time
            if elapsed > max_runtime:
                logger.warning(f"Approaching Lambda timeout after {elapsed:.1f}s. Processed {success_count} dates so far.")
                break

            date_str = current.strftime('%Y-%m-%d')
            logger.info(f"Processing date: {date_str}")

            # Run the scraper for this day
            success = run_scraper(
                year=current.year,
                month=current.month,
                day=current.day,
                **common_params
            )

            if success:
                success_count += 1
                logger.info(f"Successfully scraped date: {date_str}")
            else:
                failed_dates.append(date_str)
                logger.error(f"Failed to scrape date: {date_str}")

            current = current + timedelta(days=1)

        # Prepare result
        total_dates = (end_date - start_date).days + 1
        processed_dates = success_count + len(failed_dates)
        all_processed = processed_dates == total_dates
        all_succeeded = success_count == total_dates

        result = {
            'success': all_succeeded,
            'all_processed': all_processed,
            'total_dates': total_dates,
            'dates_processed': processed_dates,
            'success_count': success_count,
            'failed_dates': failed_dates,
            'start_date': start_date_str,
            'end_date': end_date_str,
            'storage_type': storage_type,
            'bucket_name': bucket_name,
            'architecture_version': architecture_version
        }

        logger.info(f"Scraping complete: {success_count}/{total_dates} dates succeeded")

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

    except Exception as e:
        logger.error(f"Error in handle_unified_format: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e)
            })
        }

def handle_legacy_format(event, context):
    """
    Handle the legacy format event with mode parameter

    Args:
        event (dict): Lambda event in legacy format
        context (LambdaContext): Lambda context

    Returns:
        dict: Result of the scraping operation
    """
    try:
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
            # Month mode - For month mode, translate to date range mode for unified approach
            year = int(parameters.get('year', now.year))
            month = int(parameters.get('month', now.month))

            logger.info(f"Translating month mode to date range for {year}-{month:02d}")

            # Calculate start and end dates for the month
            start_date = date(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = date(year, month, last_day)

            # Convert to unified format and process
            unified_event = {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'force_scrape': force_scrape,
                'architecture_version': architecture_version,
                'bucket_name': bucket_name,
                'html_prefix': html_prefix,
                'json_prefix': json_prefix,
                'lookup_file': lookup_file,
                'region': region,
                'max_wait': max_wait
            }

            return handle_unified_format(unified_event, context)

        elif mode == 'date_range':
            # Date range mode - translate to unified format
            start_date_str = parameters.get('start_date')
            end_date_str = parameters.get('end_date')

            if not start_date_str or not end_date_str:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'start_date and end_date are required for date_range mode'
                    })
                }

            logger.info(f"Translating date_range mode to unified format from {start_date_str} to {end_date_str}")

            # Convert to unified format and process
            unified_event = {
                'start_date': start_date_str,
                'end_date': end_date_str,
                'force_scrape': force_scrape,
                'architecture_version': architecture_version,
                'bucket_name': bucket_name,
                'html_prefix': html_prefix,
                'json_prefix': json_prefix,
                'lookup_file': lookup_file,
                'region': region,
                'max_wait': max_wait
            }

            return handle_unified_format(unified_event, context)

        else:
            # Invalid mode
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Invalid mode: {mode}. Use day, month, or date_range'
                })
            }

    except Exception as e:
        logger.error(f"Error in handle_legacy_format: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

if __name__ == "__main__":
    # For local testing - using the unified format
    result = lambda_handler({
        'start_date': '2025-02-01',
        'end_date': '2025-02-03',
        'force_scrape': True,
        'architecture_version': 'v1',
        'bucket_name': 'ncsh-app-data'
    }, None)
    print(json.dumps(result, indent=2))

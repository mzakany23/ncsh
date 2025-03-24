import os
import sys
import json
import time
import logging
import calendar
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from ncsoccer.scraper import SimpleScraper, scrape_single_date, scrape_date_range
from ncsoccer.pipeline.config import DataArchitectureVersion

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Suppress DEBUG messages from third-party libraries
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
            # Use the imported datetime module explicitly to avoid local variable shadowing
            from datetime import datetime as dt
            start_date = dt.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = dt.strptime(end_date_str, '%Y-%m-%d').date()
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
        architecture_version = "v2"  # Only support v2 architecture now
        bucket_name = event.get('bucket_name', 'ncsh-app-data')
        
        # CRITICAL: Ensure we're using clean paths without /tmp for v2 architecture
        # Remove any /tmp prefix if it exists in the provided paths
        html_prefix = event.get('html_prefix', 'v2/raw/html')
        if html_prefix.startswith('/tmp/'):
            html_prefix = html_prefix.replace('/tmp/', '')
            logger.warning(f"Removed /tmp prefix from html_prefix: {html_prefix}")
            
        json_prefix = event.get('json_prefix', 'v2/processed/json')
        if json_prefix.startswith('/tmp/'):
            json_prefix = json_prefix.replace('/tmp/', '')
            logger.warning(f"Removed /tmp prefix from json_prefix: {json_prefix}")
            
        lookup_file = event.get('lookup_file', 'v2/metadata/lookup.json')
        if lookup_file.startswith('/tmp/'):
            lookup_file = lookup_file.replace('/tmp/', '')
            logger.warning(f"Removed /tmp prefix from lookup_file: {lookup_file}")
            
        region = event.get('region', 'us-east-2')
        timeout = event.get('timeout', 10)  # 10 seconds default timeout
        max_retries = event.get('max_retries', 3)
        max_workers = event.get('max_workers', 2)  # Limit concurrency in Lambda

        # Always use S3 in Lambda
        storage_type = 's3'

        # Architecture version is always v2
        arch_version = DataArchitectureVersion("v2")
        logger.info(f"Using architecture version: {arch_version.value}")

        # Record start time for timeout tracking
        start_time = time.time()
        max_runtime = 25  # 25 seconds = strict 30 second timeout - 5 second buffer

        # Create scraper for date range
        scraper = SimpleScraper(
            mode='range',
            start_year=start_date.year,
            start_month=start_date.month,
            start_day=start_date.day,
            end_year=end_date.year,
            end_month=end_date.month,
            end_day=end_date.day,
            storage_type=storage_type,
            bucket_name=bucket_name,
            html_prefix=html_prefix,
            json_prefix=json_prefix,
            lookup_file=lookup_file,
            lookup_type='s3',
            region=region,
            force_scrape=force_scrape,
            architecture_version=architecture_version,
            max_workers=max_workers,
            timeout=timeout,
            max_retries=max_retries
        )

        # Process date range
        results = scraper.scrape_date_range(start_date, end_date)

        # Calculate statistics
        total_dates = (end_date - start_date).days + 1
        success_count = sum(1 for success in results.values() if success)
        failed_dates = [date for date, success in results.items() if not success]
        all_succeeded = success_count == total_dates

        # Create detailed results object
        detailed_results = {
            'success': all_succeeded,
            'all_processed': len(results) == total_dates,
            'total_dates': total_dates,
            'dates_processed': len(results),
            'success_count': success_count,
            'failed_dates': failed_dates,
            'start_date': start_date_str,
            'end_date': end_date_str,
            'storage_type': storage_type,
            'bucket_name': bucket_name,
            'architecture_version': architecture_version,
            'execution_time_seconds': time.time() - start_time,
            'games_scraped': scraper.games_scraped,
            'detailed_results': results
        }
        
        # Store detailed results in S3
        import boto3
        import uuid
        from datetime import datetime
        
        s3 = boto3.client('s3')
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        batch_id = str(uuid.uuid4())[:8]  # Use a short UUID for the batch ID
        results_key = f"{architecture_version}/metadata/batch_results/{start_date_str}_to_{end_date_str}_{timestamp}_{batch_id}.json"
        
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=results_key,
                Body=json.dumps(detailed_results),
                ContentType='application/json'
            )
            logger.info(f"Stored detailed batch results in S3: s3://{bucket_name}/{results_key}")
            
            # Return minimal result with reference to S3
            result = {
                'success': all_succeeded,
                'total_dates': total_dates,
                'success_count': success_count,
                'start_date': start_date_str,
                'end_date': end_date_str,
                'results_s3_bucket': bucket_name,
                'results_s3_key': results_key
            }
        except Exception as e:
            logger.error(f"Failed to store detailed results in S3: {str(e)}")
            # Fall back to returning minimal results without S3 reference
            result = {
                'success': all_succeeded,
                'total_dates': total_dates,
                'success_count': success_count,
                'start_date': start_date_str,
                'end_date': end_date_str,
                'error_storing_results': str(e)
            }

        logger.info(f"Scraping complete: {success_count}/{total_dates} dates succeeded in {time.time() - start_time:.2f} seconds")

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
        # Extract mode and parameters
        mode = event.get('mode', 'day')
        parameters = event.get('parameters', {})

        # Get common parameters with defaults
        force_scrape = parameters.get('force_scrape', False)
        architecture_version = "v2"  # Only v2 is supported now
        bucket_name = parameters.get('bucket_name', 'ncsh-app-data')
        html_prefix = parameters.get('html_prefix', 'v2/raw/html')
        json_prefix = parameters.get('json_prefix', 'v2/processed/json')
        lookup_file = parameters.get('lookup_file', 'v2/metadata/lookup.json')
        region = parameters.get('region', 'us-east-2')
        timeout = parameters.get('timeout', 10)
        max_retries = parameters.get('max_retries', 3)
        max_workers = parameters.get('max_workers', 2)

        # Always use S3 in Lambda
        storage_type = 's3'

        # Common parameters for all modes
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
            'timeout': timeout,
            'max_retries': max_retries,
            'max_workers': max_workers
        }

        start_time = time.time()
        result = {}

        if mode == 'day':
            # Single day mode
            year = int(parameters.get('year', datetime.now().year))
            month = int(parameters.get('month', datetime.now().month))
            day = int(parameters.get('day', datetime.now().day))

            # Create scraper for single day
            scraper = SimpleScraper(
                mode='day',
                year=year,
                month=month,
                day=day,
                **common_params
            )

            # Run scraper
            success = scraper.run()

            result = {
                'success': success,
                'mode': 'day',
                'year': year,
                'month': month,
                'day': day,
                'games_scraped': scraper.games_scraped,
                'execution_time_seconds': time.time() - start_time
            }

        elif mode == 'month':
            # Month mode
            year = int(parameters.get('year', datetime.now().year))
            month = int(parameters.get('month', datetime.now().month))

            # Determine first and last day of month
            first_day = 1
            if month == 12:
                last_day = 31
            else:
                last_day = (datetime(year, month + 1, 1) - timedelta(days=1)).day

            # Create date objects
            start_date = datetime(year, month, first_day).date()
            end_date = datetime(year, month, last_day).date()

            # Create scraper for date range
            scraper = SimpleScraper(
                mode='range',
                start_year=year,
                start_month=month,
                start_day=first_day,
                end_year=year,
                end_month=month,
                end_day=last_day,
                **common_params
            )

            # Run scraper
            results = scraper.scrape_date_range(start_date, end_date)

            # Calculate statistics
            total_dates = last_day
            success_count = sum(1 for success in results.values() if success)
            failed_dates = [date for date, success in results.items() if not success]
            all_succeeded = success_count == total_dates

            result = {
                'success': all_succeeded,
                'mode': 'month',
                'year': year,
                'month': month,
                'total_dates': total_dates,
                'success_count': success_count,
                'failed_dates': failed_dates,
                'games_scraped': scraper.games_scraped,
                'execution_time_seconds': time.time() - start_time
            }

        elif mode == 'date_range':
            # Date range mode
            start_date_str = parameters.get('start_date')
            end_date_str = parameters.get('end_date')

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

            # Create scraper for date range
            scraper = SimpleScraper(
                mode='range',
                start_year=start_date.year,
                start_month=start_date.month,
                start_day=start_date.day,
                end_year=end_date.year,
                end_month=end_date.month,
                end_day=end_date.day,
                **common_params
            )

            # Run scraper
            results = scraper.scrape_date_range(start_date, end_date)

            # Calculate statistics
            total_dates = (end_date - start_date).days + 1
            success_count = sum(1 for success in results.values() if success)
            failed_dates = [date for date, success in results.items() if not success]
            all_succeeded = success_count == total_dates

            result = {
                'success': all_succeeded,
                'mode': 'date_range',
                'start_date': start_date_str,
                'end_date': end_date_str,
                'total_dates': total_dates,
                'success_count': success_count,
                'failed_dates': failed_dates,
                'games_scraped': scraper.games_scraped,
                'execution_time_seconds': time.time() - start_time
            }

        else:
            error_msg = f"Invalid mode: {mode}. Must be one of: day, month, date_range"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'error': error_msg
                })
            }

        # Add common fields to result
        result.update({
            'storage_type': storage_type,
            'bucket_name': bucket_name,
            'architecture_version': architecture_version
        })

        logger.info(f"Scraping complete in {time.time() - start_time:.2f} seconds")

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

    except Exception as e:
        logger.error(f"Error in handle_legacy_format: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e)
            })
        }

if __name__ == "__main__":
    # For local testing
    test_event = {
        "start_date": "2025-03-01",
        "end_date": "2025-03-05",
        "force_scrape": True,
        "architecture_version": "v2",
        "bucket_name": "ncsh-app-data"
    }
    print(json.dumps(lambda_handler(test_event, None), indent=2))

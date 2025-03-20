import os
import sys
import json
import logging
import calendar
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from ncsoccer.runner import run_scraper, run_month
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
                    "end_date": "2024-01-31"     # For date_range mode
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

        # Extract architecture version (defaults to v1 for backward compatibility)
        architecture_version = parameters.get('architecture_version', 'v1')
        logger.info(f"Using data architecture version: {architecture_version}")

        # Extract max_wait parameter if provided, otherwise use default value of 300 seconds
        max_wait = int(parameters.get('max_wait', 300))
        logger.info(f"Using max_wait value: {max_wait} seconds")

        # Validate architecture version
        try:
            arch_version = DataArchitectureVersion(architecture_version.lower())
            logger.info(f"Architecture version validated: {arch_version.value}")
        except ValueError:
            logger.warning(f"Invalid architecture_version: {architecture_version}. Using v1 as default.")
            architecture_version = 'v1'

        # Get current date for defaults
        now = datetime.now()

        # Handle different modes
        if mode == 'day':
            # Day mode - scrape a single day
            year = int(parameters.get('year', now.year))
            month = int(parameters.get('month', now.month))
            day = int(parameters.get('day', now.day))
            force_scrape = parameters.get('force_scrape', False)

            logger.info(f"Running in day mode for {year}-{month:02d}-{day:02d}")
            logger.info(f"Configuration: force_scrape={force_scrape}, architecture_version={architecture_version}")

            result = run_scraper(
                year=year,
                month=month,
                day=day,
                force_scrape=force_scrape,
                architecture_version=architecture_version,
                max_wait=max_wait
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': result,
                    'date': f"{year}-{month:02d}-{day:02d}",
                    'architecture_version': architecture_version
                })
            }

        elif mode == 'month':
            # Month mode - scrape a whole month
            year = int(parameters.get('year', now.year))
            month = int(parameters.get('month', now.month))
            force_scrape = parameters.get('force_scrape', False)

            logger.info(f"Running in month mode for {year}-{month:02d}")
            logger.info(f"Configuration: force_scrape={force_scrape}, architecture_version={architecture_version}")

            result = run_month(
                year=year,
                month=month,
                force_scrape=force_scrape,
                architecture_version=architecture_version,
                max_wait=max_wait
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': result,
                    'year': year,
                    'month': month,
                    'architecture_version': architecture_version
                })
            }

        elif mode == 'date_range':
            # Date range mode - scrape a range of dates
            start_date_str = parameters.get('start_date')
            end_date_str = parameters.get('end_date')
            force_scrape = parameters.get('force_scrape', False)

            if not start_date_str or not end_date_str:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'start_date and end_date are required for date_range mode'
                    })
                }

            logger.info(f"Running in date_range mode from {start_date_str} to {end_date_str}")
            logger.info(f"Configuration: force_scrape={force_scrape}, architecture_version={architecture_version}")

            # Parse dates
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            except ValueError as e:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': f'Invalid date format: {str(e)}. Use YYYY-MM-DD'
                    })
                }

            # Check if we have enough time to process the entire range
            remaining_time_ms = context.get_remaining_time_in_millis() if context else 900000
            remaining_time_sec = remaining_time_ms / 1000

            # Estimate time needed - 30 seconds per month plus buffer
            days_to_process = (end_date - start_date).days + 1
            estimated_time_per_day = 30  # seconds
            estimated_time_needed = days_to_process * estimated_time_per_day

            # Process as many months as we can fit
            processed_months = []
            current_date = start_date

            # Use a date_range-specific runner that can process multiple months efficiently
            while current_date <= end_date and (remaining_time_sec - estimated_time_needed) > 60:
                current_month = (current_date.year, current_date.month)

                # Only process each month once
                if current_month not in processed_months:
                    # Calculate last day of this month that's within range
                    if current_date.month == 12:
                        next_month = datetime(current_date.year + 1, 1, 1)
                    else:
                        next_month = datetime(current_date.year, current_date.month + 1, 1)

                    # Make sure we don't go past end_date
                    month_end = min(next_month - timedelta(days=1), end_date)

                    # Calculate target days for this month
                    if current_date.day == 1 and month_end.day == calendar.monthrange(month_end.year, month_end.month)[1]:
                        # Whole month, use run_month
                        logger.info(f"Processing full month: {current_date.year}-{current_date.month:02d}")
                        result = run_month(
                            year=current_date.year,
                            month=current_date.month,
                            force_scrape=force_scrape,
                            architecture_version=architecture_version,
                            max_wait=max_wait
                        )
                    else:
                        # Partial month, calculate days to process
                        target_days = list(range(current_date.day, month_end.day + 1))
                        logger.info(f"Processing partial month: {current_date.year}-{current_date.month:02d}, days: {target_days}")
                        result = run_month(
                            year=current_date.year,
                            month=current_date.month,
                            target_days=target_days,
                            force_scrape=force_scrape,
                            architecture_version=architecture_version,
                            max_wait=max_wait
                        )

                    processed_months.append(current_month)

                    # Update time estimates based on actual time used
                    remaining_time_ms = context.get_remaining_time_in_millis() if context else remaining_time_ms - 30000
                    remaining_time_sec = remaining_time_ms / 1000

                # Move to next month
                if current_date.month == 12:
                    current_date = datetime(current_date.year + 1, 1, 1)
                else:
                    current_date = datetime(current_date.year, current_date.month + 1, 1)

                # Check if we're done
                if current_date > end_date:
                    break

            # Report success if we processed everything, partial success otherwise
            complete = current_date > end_date

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': True,
                    'complete': complete,
                    'processed_months': processed_months,
                    'start_date': start_date_str,
                    'end_date': end_date_str,
                    'remaining_time_ms': remaining_time_ms,
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
            'architecture_version': 'v2'
        }
    }, None)
    print(json.dumps(result, indent=2))

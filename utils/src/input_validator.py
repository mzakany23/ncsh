import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Validates input parameters for the workflow

    Args:
        event (dict): Contains start_date, end_date, and other parameters

    Returns:
        dict: Validated and normalized input parameters
    """
    try:
        logger.info(f"Validating input parameters: {json.dumps(event)}")

        # Required parameters
        start_date = event.get('start_date')
        end_date = event.get('end_date')

        if not start_date or not end_date:
            logger.error("Missing required parameters: start_date and end_date")
            return {
                'statusCode': 400,
                'error': 'Missing required parameters: start_date and end_date are required'
            }

        # Parse dates to validate format
        try:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
        except ValueError as e:
            logger.error(f"Invalid date format: {str(e)}")
            return {
                'statusCode': 400,
                'error': f'Invalid date format: {str(e)}. Use YYYY-MM-DD format.'
            }

        # Check that start_date <= end_date
        if start_date_obj > end_date_obj:
            logger.error(f"Start date {start_date} is after end date {end_date}")
            return {
                'statusCode': 400,
                'error': f'start_date ({start_date}) must be before or equal to end_date ({end_date})'
            }

        # Optional parameters with defaults
        force_scrape = event.get('force_scrape', False)
        architecture_version = "v2"  # Only v2 is supported now
        batch_size = int(event.get('batch_size', 3))
        bucket_name = event.get('bucket_name', 'ncsh-app-data')

        # Validate batch_size (between 1 and 10)
        if batch_size < 1 or batch_size > 10:
            logger.warning(f"Invalid batch_size: {batch_size}. Adjusting to valid range (1-10).")
            batch_size = max(1, min(batch_size, 10))

        validated_input = {
            'start_date': start_date,
            'end_date': end_date,
            'force_scrape': force_scrape,
            'architecture_version': architecture_version,
            'batch_size': batch_size,
            'bucket_name': bucket_name
        }

        logger.info(f"Validation successful: {json.dumps(validated_input)}")
        return validated_input

    except Exception as e:
        logger.error(f"Error validating input: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'error': f'Input validation error: {str(e)}'
        }
import json
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Plans batches for date range processing

    Args:
        event (dict): Contains start_date, end_date, and batch_size

    Returns:
        dict: List of date range batches to process
    """
    try:
        start_date_str = event.get('start_date')
        end_date_str = event.get('end_date')
        batch_size = int(event.get('batch_size', 3))

        logger.info(f"Planning batches for date range: {start_date_str} to {end_date_str} with batch size {batch_size}")

        # Parse dates
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

        # Calculate total days
        total_days = (end_date - start_date).days + 1
        logger.info(f"Total days to process: {total_days}")

        # Plan batches
        batches = []
        current_date = start_date

        while current_date <= end_date:
            batch_end = min(current_date + timedelta(days=batch_size-1), end_date)

            batches.append({
                'start_date': current_date.strftime('%Y-%m-%d'),
                'end_date': batch_end.strftime('%Y-%m-%d'),
                'days': (batch_end - current_date).days + 1
            })

            current_date = batch_end + timedelta(days=1)

        logger.info(f"Created {len(batches)} batches")
        for i, batch in enumerate(batches):
            logger.info(f"Batch {i+1}: {batch['start_date']} to {batch['end_date']} ({batch['days']} days)")

        return {
            'total_days': total_days,
            'batch_count': len(batches),
            'batches': batches
        }

    except Exception as e:
        logger.error(f"Error planning batches: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'error': f'Batch planning error: {str(e)}'
        }
import json
import logging
import boto3
import os
from datetime import datetime, timedelta
from dateutil import parser

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Verifies all batches completed successfully by checking S3 for processed files
    instead of relying on batch_results from the state machine

    Args:
        event (dict): Contains start_date, end_date, bucket_name, and architecture_version

    Returns:
        dict: Verification result
    """
    try:
        # Extract parameters from the event
        start_date = event.get('start_date')
        end_date = event.get('end_date')
        bucket_name = event.get('bucket_name', 'ncsh-app-data')
        architecture_version = event.get('architecture_version', 'v2')
        
        logger.info(f"Verifying batches for date range: {start_date} to {end_date}")
        
        # Convert date strings to datetime objects
        start_dt = parser.parse(start_date).date()
        end_dt = parser.parse(end_date).date()
        
        # Calculate the number of days in the date range
        date_range_days = (end_dt - start_dt).days + 1
        logger.info(f"Date range contains {date_range_days} days")
        
        # Initialize S3 client
        s3 = boto3.client('s3')
        
        # Check for processed files in S3
        processed_dates = []
        missing_dates = []
        
        current_dt = start_dt
        while current_dt <= end_dt:
            year = current_dt.year
            month = current_dt.month
            day = current_dt.day
            
            # Construct the S3 prefix for this date
            prefix = f"{architecture_version}/processed/json/year={year}/month={month:02d}/day={day:02d}/"
            
            try:
                # Check if files exist for this date
                response = s3.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    MaxKeys=1
                )
                
                if 'Contents' in response and len(response['Contents']) > 0:
                    processed_dates.append(current_dt.strftime('%Y-%m-%d'))
                    logger.info(f"Found processed files for {current_dt.strftime('%Y-%m-%d')}")
                else:
                    missing_dates.append(current_dt.strftime('%Y-%m-%d'))
                    logger.warning(f"No processed files found for {current_dt.strftime('%Y-%m-%d')}")
            except Exception as e:
                logger.error(f"Error checking S3 for {current_dt.strftime('%Y-%m-%d')}: {str(e)}")
                missing_dates.append(current_dt.strftime('%Y-%m-%d'))
            
            # Move to the next day
            current_dt += timedelta(days=1)
        
        # Determine success based on whether all dates were processed
        success = len(missing_dates) == 0
        
        result = {
            'success': success,
            'total_days': date_range_days,
            'processed_days': len(processed_dates),
            'missing_days': missing_dates
        }

        logger.info(f"Verification complete: {'Success' if success else 'Failed'}")
        logger.info(f"Processed {len(processed_dates)} days out of {date_range_days} total days")

        if not success:
            logger.warning(f"Missing dates: {len(missing_dates)}")
            for date in missing_dates:
                logger.warning(f"  Missing date: {date}")

        return result

    except Exception as e:
        logger.error(f"Error verifying batches: {str(e)}", exc_info=True)
        return {
            'success': False,
            'error': f'Batch verification error: {str(e)}'
        }
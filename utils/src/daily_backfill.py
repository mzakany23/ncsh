"""
Daily Backfill Handler

This module contains the handler for the daily backfill Lambda function.
It triggers the recursive workflow for the last 3 days to ensure complete data.
"""

import os
import logging
import json
import boto3
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Handler for the daily backfill Lambda function.
    Triggers the recursive workflow for the last 3 days.

    Args:
        event (dict): Lambda event
        context (LambdaContext): Lambda context

    Returns:
        dict: Result from the operation
    """
    try:
        logger.info("Starting daily backfill handler")
        logger.info(f"Event: {json.dumps(event)}")
        logger.info(f"Context: {context}")
        
        # Get environment variables
        state_machine_arn = os.environ.get('STATE_MACHINE_ARN')
        bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
        architecture_version = os.environ.get('ARCHITECTURE_VERSION', 'v2')
        force_scrape = os.environ.get('FORCE_SCRAPE', 'true').lower() == 'true'
        batch_size = int(os.environ.get('BATCH_SIZE', '1'))
        
        # Log all environment variables for debugging
        logger.info(f"STATE_MACHINE_ARN: {state_machine_arn}")
        logger.info(f"DATA_BUCKET: {bucket_name}")
        logger.info(f"ARCHITECTURE_VERSION: {architecture_version}")
        logger.info(f"FORCE_SCRAPE: {force_scrape}")
        logger.info(f"BATCH_SIZE: {batch_size}")
        
        # Calculate date range for last 3 days
        today = datetime.now()
        end_date = today.strftime('%Y-%m-%d')
        start_date = (today - timedelta(days=2)).strftime('%Y-%m-%d')
        
        logger.info(f"Date range: {start_date} to {end_date}")
        
        # Prepare input for the recursive workflow
        input_data = {
            "start_date": start_date,
            "end_date": end_date,
            "force_scrape": force_scrape,
            "batch_size": batch_size,
            "bucket_name": bucket_name,
            "architecture_version": architecture_version,
            "is_sub_execution": False
        }
        
        logger.info(f"Input data: {json.dumps(input_data)}")
        
        # Trigger the step function
        client = boto3.client('stepfunctions')
        execution_name = f"daily-backfill-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        
        response = client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(input_data)
        )
        
        logger.info(f"Started execution: {response['executionArn']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Daily backfill triggered successfully',
                'execution_arn': response['executionArn'],
                'start_date': start_date,
                'end_date': end_date
            })
        }
    except Exception as e:
        logger.error(f"Error in daily backfill handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Error in daily backfill handler: {str(e)}'
            })
        }

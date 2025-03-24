import json
import logging
import os
import boto3
from datetime import datetime, timedelta
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Splits a large date range into smaller chunks and triggers separate Step Function executions.
    
    Args:
        event (dict): Contains start_date, end_date, and other parameters
        context (object): Lambda context
        
    Returns:
        dict: Information about the sub-executions that were triggered
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Extract parameters from event
    start_date_str = event.get('start_date')
    end_date_str = event.get('end_date')
    max_chunk_size_days = event.get('max_chunk_size_days', 90)  # Default to 90 days per chunk
    bucket_name = event.get('bucket_name')
    force_scrape = event.get('force_scrape', False)
    architecture_version = event.get('architecture_version', 'v2')
    batch_size = event.get('batch_size', 3)
    
    # Convert dates to datetime objects
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    # Calculate total days in the range
    total_days = (end_date - start_date).days + 1
    
    # If the range is small enough, return that it should be processed directly
    if total_days <= max_chunk_size_days:
        logger.info(f"Date range is within limits ({total_days} days). Processing directly.")
        return {
            'split_required': False,
            'original_range': {
                'start_date': start_date_str,
                'end_date': end_date_str
            }
        }
    
    # Split the date range into chunks
    chunks = []
    current_start = start_date
    
    while current_start <= end_date:
        # Calculate the end date for this chunk
        chunk_end = min(current_start + timedelta(days=max_chunk_size_days - 1), end_date)
        
        chunks.append({
            'start_date': current_start.strftime('%Y-%m-%d'),
            'end_date': chunk_end.strftime('%Y-%m-%d')
        })
        
        # Move to the next chunk
        current_start = chunk_end + timedelta(days=1)
    
    logger.info(f"Split date range into {len(chunks)} chunks")
    
    # Trigger Step Function executions for each chunk
    step_functions_client = boto3.client('stepfunctions')
    state_machine_arn = os.environ.get('STATE_MACHINE_ARN')
    
    if not state_machine_arn:
        raise ValueError("STATE_MACHINE_ARN environment variable is not set")
    
    executions = []
    
    for i, chunk in enumerate(chunks):
        execution_name = f"split-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}-{i}-{uuid.uuid4().hex[:8]}"
        
        # Prepare input for the Step Function
        input_data = {
            'start_date': chunk['start_date'],
            'end_date': chunk['end_date'],
            'force_scrape': force_scrape,
            'batch_size': batch_size,
            'bucket_name': bucket_name,
            'architecture_version': architecture_version,
            'is_sub_execution': True,  # Flag to indicate this is a sub-execution
            'parent_execution_id': context.aws_request_id
        }
        
        # Start execution
        response = step_functions_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(input_data)
        )
        
        executions.append({
            'execution_arn': response['executionArn'],
            'start_date': chunk['start_date'],
            'end_date': chunk['end_date']
        })
        
        logger.info(f"Started execution {execution_name} for chunk {i+1}/{len(chunks)}")
    
    return {
        'split_required': True,
        'original_range': {
            'start_date': start_date_str,
            'end_date': end_date_str
        },
        'chunks': chunks,
        'executions': executions
    }

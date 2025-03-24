import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Checks the status of Step Function sub-executions.
    
    Args:
        event (dict): Contains list of execution ARNs to check
        context (object): Lambda context
        
    Returns:
        dict: Status of all sub-executions
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Extract execution ARNs from event
    executions = event.get('executions', [])
    
    if not executions:
        logger.warning("No executions to check")
        return {
            'success': True,
            'message': 'No executions to check',
            'executions': []
        }
    
    # Check status of each execution
    step_functions_client = boto3.client('stepfunctions')
    execution_results = []
    all_succeeded = True
    
    for execution in executions:
        execution_arn = execution.get('execution_arn')
        
        try:
            response = step_functions_client.describe_execution(
                executionArn=execution_arn
            )
            
            status = response['status']
            execution_result = {
                'execution_arn': execution_arn,
                'status': status,
                'start_date': execution.get('start_date'),
                'end_date': execution.get('end_date')
            }
            
            # Add output if execution is complete
            if status in ['SUCCEEDED', 'FAILED']:
                if status == 'SUCCEEDED':
                    execution_result['output'] = json.loads(response.get('output', '{}'))
                else:
                    execution_result['error'] = response.get('error', 'Unknown error')
                    execution_result['cause'] = response.get('cause', 'Unknown cause')
                    all_succeeded = False
            else:
                # Execution is still running
                all_succeeded = False
            
            execution_results.append(execution_result)
            
        except Exception as e:
            logger.error(f"Error checking execution {execution_arn}: {str(e)}")
            execution_results.append({
                'execution_arn': execution_arn,
                'status': 'ERROR',
                'error': str(e),
                'start_date': execution.get('start_date'),
                'end_date': execution.get('end_date')
            })
            all_succeeded = False
    
    # Count statuses
    status_counts = {}
    for result in execution_results:
        status = result.get('status')
        status_counts[status] = status_counts.get(status, 0) + 1
    
    return {
        'success': all_succeeded,
        'message': 'All executions completed successfully' if all_succeeded else 'Some executions are still running or have failed',
        'status_counts': status_counts,
        'executions': execution_results
    }

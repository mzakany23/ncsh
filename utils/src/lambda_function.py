import os
import logging
import json
from input_validator import handler as input_validator_handler
from batch_planner import handler as batch_planner_handler
from batch_verifier import handler as batch_verifier_handler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Map of function names to handlers
HANDLER_MAP = {
    'ncsoccer_input_validator': input_validator_handler,
    'ncsoccer_batch_planner': batch_planner_handler,
    'ncsoccer_batch_verifier': batch_verifier_handler
}

def handler(event, context):
    """
    Main handler that routes to the appropriate function handler
    based on the Lambda function name.

    Args:
        event (dict): Lambda event
        context (LambdaContext): Lambda context

    Returns:
        dict: Result from the appropriate handler
    """
    try:
        # Get the Lambda function name from the environment
        function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
        logger.info(f"Function name: {function_name}")

        # Route to the appropriate handler
        if function_name in HANDLER_MAP:
            logger.info(f"Routing to handler for {function_name}")
            return HANDLER_MAP[function_name](event, context)
        else:
            logger.error(f"Unknown function name: {function_name}")
            return {
                'statusCode': 500,
                'error': f'Unknown function name: {function_name}'
            }
    except Exception as e:
        logger.error(f"Error in handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'error': f'Error in handler: {str(e)}'
        }

if __name__ == "__main__":
    # Test the handlers locally
    test_event = {
        'start_date': '2025-01-01',
        'end_date': '2025-01-10',
        'batch_size': 3,
        'force_scrape': True,
        'architecture_version': 'v1',
        'bucket_name': 'ncsh-app-data'
    }

    # Test input validator
    print("\nTesting input validator:")
    result = input_validator_handler(test_event, None)
    print(json.dumps(result, indent=2))

    # Test batch planner
    print("\nTesting batch planner:")
    result = batch_planner_handler(test_event, None)
    print(json.dumps(result, indent=2))

    # Test batch verifier with mock batch results
    print("\nTesting batch verifier:")
    mock_batch_results = {
        'batch_results': [
            {
                'Payload': {
                    'body': json.dumps({
                        'success': True,
                        'dates_processed': 3,
                        'start_date': '2025-01-01',
                        'end_date': '2025-01-03'
                    })
                }
            },
            {
                'Payload': {
                    'body': json.dumps({
                        'success': True,
                        'dates_processed': 3,
                        'start_date': '2025-01-04',
                        'end_date': '2025-01-06'
                    })
                }
            },
            {
                'Payload': {
                    'body': json.dumps({
                        'success': True,
                        'dates_processed': 4,
                        'start_date': '2025-01-07',
                        'end_date': '2025-01-10'
                    })
                }
            }
        ]
    }
    result = batch_verifier_handler(mock_batch_results, None)
    print(json.dumps(result, indent=2))
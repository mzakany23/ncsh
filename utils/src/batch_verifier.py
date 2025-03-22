import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Verifies all batches completed successfully

    Args:
        event (dict): Contains results from all batch executions

    Returns:
        dict: Verification result
    """
    try:
        batch_results = event.get('batch_results', [])

        logger.info(f"Verifying {len(batch_results)} batch results")

        failed_batches = []
        success_count = 0
        total_processed = 0

        # Check each batch result
        for i, result in enumerate(batch_results):
            try:
                if 'Payload' not in result:
                    logger.error(f"Batch {i+1}: Missing Payload in result")
                    failed_batches.append({
                        'batch_index': i,
                        'error': 'Missing Payload in result'
                    })
                    continue

                payload = result.get('Payload', {})

                # Handle both types of response formats
                if 'body' in payload:
                    # Standard Lambda proxy response
                    body = json.loads(payload.get('body', '{}'))
                    success = body.get('success', False)

                    if success:
                        success_count += 1
                        batch_processed = body.get('dates_processed', 0)
                        total_processed += batch_processed
                        logger.info(f"Batch {i+1}: Success - Processed {batch_processed} dates")
                    else:
                        failed_batches.append({
                            'batch_index': i,
                            'start_date': body.get('start_date'),
                            'end_date': body.get('end_date'),
                            'error': body.get('error', 'Unknown error')
                        })
                        logger.warning(f"Batch {i+1}: Failed - {body.get('error', 'Unknown error')}")
                else:
                    # Direct response
                    success = payload.get('success', False)

                    if success:
                        success_count += 1
                        batch_processed = payload.get('dates_processed', 0)
                        total_processed += batch_processed
                        logger.info(f"Batch {i+1}: Success - Processed {batch_processed} dates")
                    else:
                        failed_batches.append({
                            'batch_index': i,
                            'start_date': payload.get('start_date'),
                            'end_date': payload.get('end_date'),
                            'error': payload.get('error', 'Unknown error')
                        })
                        logger.warning(f"Batch {i+1}: Failed - {payload.get('error', 'Unknown error')}")
            except Exception as e:
                failed_batches.append({
                    'batch_index': i,
                    'error': f"Failed to parse batch result: {str(e)}"
                })
                logger.error(f"Error parsing batch {i+1} result: {str(e)}", exc_info=True)

        success = len(failed_batches) == 0

        result = {
            'success': success,
            'total_batches': len(batch_results),
            'successful_batches': success_count,
            'total_dates_processed': total_processed,
            'failed_batches': failed_batches
        }

        logger.info(f"Verification complete: {'Success' if success else 'Failed'}")
        logger.info(f"Processed {total_processed} dates across {success_count} successful batches")

        if not success:
            logger.warning(f"Failed batches: {len(failed_batches)}")
            for i, batch in enumerate(failed_batches):
                logger.warning(f"  Failed batch {i+1}: {batch}")

        return result

    except Exception as e:
        logger.error(f"Error verifying batches: {str(e)}", exc_info=True)
        return {
            'success': False,
            'error': f'Batch verification error: {str(e)}'
        }
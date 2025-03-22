# Unified Date Range Workflow with Batching

This document describes the unified date range workflow with batching that replaces the previous separate day and month modes of the scraper.

## Overview

The unified workflow simplifies the scraping process by consolidating multiple operation modes into a single date range approach. This design provides:

1. **Conceptual simplicity**: A unified interface for all scraping operations
2. **Improved reliability**: Batch processing to prevent Lambda timeouts
3. **Enhanced scalability**: Process dates in parallel with managed concurrency
4. **Built-in error handling**: Retry mechanisms and partial success capabilities

## Architecture

The workflow consists of the following components:

### Step Function Components

1. **Input Validator Lambda**: Validates input parameters and formats dates
2. **Batch Planner Lambda**: Divides the date range into manageable batches
3. **Map State**: Processes each batch in parallel
4. **Scraper Lambda**: Performs the actual scraping for each day in a batch
5. **Batch Verifier Lambda**: Validates results and aggregates metrics

### Input Schema

The workflow accepts the following input:

```json
{
  "start_date": "YYYY-MM-DD",    // Start date of the range
  "end_date": "YYYY-MM-DD",      // End date of the range
  "force_scrape": false,         // Optional: Force re-scraping of data
  "architecture_version": "v1",  // Optional: Data architecture version
  "batch_size": 3,               // Optional: Number of days per batch
  "bucket_name": "ncsh-app-data" // Optional: S3 bucket name
}
```

### Workflow Execution

1. **Input Validation**: Validates date formats and checks that start_date precedes end_date
2. **Batch Planning**: Divides the date range into batches of size `batch_size`
3. **Parallel Execution**: Each batch is processed in parallel
4. **Scraping**: Individual dates within a batch are processed sequentially
5. **Verification**: Results are verified and metrics are collected

## Using the Workflow

### Trigger Script

A script is provided to easily trigger the workflow:

```bash
python scripts/trigger_batched_workflow.py --date-range 2024-03-01 2024-03-31
```

See the README for more usage examples.

### Monitoring

You can monitor the workflow execution in the AWS Step Functions console. The trigger script will output a link to the console:

```
Console URL: https://us-east-2.console.aws.amazon.com/states/home?region=us-east-2#/executions/details/arn:aws:states:us-east-2:552336166511:execution:ncsoccer-unified-workflow-batched:scraper-2024-03-01-12-34-56
```

### Execution Metrics

Each execution will provide metrics such as:
- Total days processed
- Success rate
- Error details (if any)
- Execution time

## Error Handling

The workflow includes several error handling mechanisms:

1. **Input validation**: Prevents execution with invalid parameters
2. **Batch level retries**: Each batch can be retried independently
3. **Robust error reporting**: Detailed error information for debugging
4. **Partial success**: Even if some dates fail, others can still succeed

## Implementation Details

### Lambda Functions

1. **Input Validator** (`input-validator`):
   - Validates date formats
   - Ensures start_date ≤ end_date
   - Normalizes input parameters

2. **Batch Planner** (`batch-planner`):
   - Calculates number of days in range
   - Divides range into batches
   - Generates execution plan

3. **Scraper** (`scraper`):
   - Performs actual scraping
   - Stores HTML and parsed data
   - Reports success/failure

4. **Batch Verifier** (`batch-verifier`):
   - Aggregates batch results
   - Validates data consistency
   - Generates execution summary

### Terraform Configuration

The infrastructure is defined using Terraform in:
- `terraform/infrastructure/lambda-batched.tf`
- `terraform/infrastructure/unified-workflow-batched.asl.json`

## Advantages Over Previous Design

1. **Simplified interface**: One unified mode instead of three separate modes
2. **Improved reliability**: Batching prevents Lambda timeouts
3. **Better error handling**: More granular retry mechanisms
4. **Enhanced monitoring**: Detailed metrics per execution
5. **Scalable architecture**: Process large date ranges efficiently
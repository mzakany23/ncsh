# NC Soccer Data Pipeline

A data pipeline for collecting and processing soccer game data.

## Data Architecture

This project uses the v2 data architecture, which provides:

- Organized directory structure for better data management
- Enhanced performance in data processing
- Clear separation of JSON and Parquet data
- Standardized file naming conventions

## Setup

1. Install dependencies:
```bash
make install
```

2. Run tests:
```bash
make test
```

## Usage

### Running the Recursive Workflow

The recursive workflow processes soccer game data for a specified date range with automatic batching for improved reliability and performance. This workflow can handle large date ranges (e.g., multiple years) efficiently.

```bash
# Process data for a specific date range
python scripts/trigger_recursive_workflow.py --date-range 2021-01-01 2023-12-31

# Optional: Specify batch size (default is 10 days)
python scripts/trigger_recursive_workflow.py --date-range 2021-01-01 2023-12-31 --batch-size 10

# Optional: Force re-scrape of existing data
python scripts/trigger_recursive_workflow.py --date-range 2021-01-01 2023-12-31 --force-scrape

# Optional: Use a specific AWS profile
python scripts/trigger_recursive_workflow.py --date-range 2021-01-01 2023-12-31 --profile your-profile-name

# Process a single date
python scripts/trigger_recursive_workflow.py --date 2023-01-15

# Process an entire month
python scripts/trigger_recursive_workflow.py --month 2023 1
```

#### Local Testing with Workflow Simulator

For development and testing, you can use the local workflow simulator:

```bash
# Process data for a specific day
python scripts/local_workflow_simulator.py --year 2021 --month 1 --day 15 --mode day

# Process data for an entire month
python scripts/local_workflow_simulator.py --year 2021 --month 1 --mode month
```

Local execution stores data in the `./data` directory and execution logs in `./test_output`.

### Parameters Explained

#### Recursive Workflow Parameters

| Parameter | Description |
| --------- | ----------- |
| `--date-range START_DATE END_DATE` | Specifies the start and end dates for data processing in YYYY-MM-DD format. The workflow will process all dates inclusive of both start and end dates. |
| `--date YYYY-MM-DD` | Process a single date. |
| `--month YYYY MM` | Process an entire month (e.g., `--month 2021 3` for March 2021). |
| `--batch-size DAYS` | Number of days to include in each batch. The recursive workflow automatically handles batching for large date ranges. Default is 10 days. |
| `--force-scrape` | Forces the workflow to re-scrape and re-process data even if it already exists in the destination. Without this flag, the workflow skips dates that have already been processed. |
| `--profile PROFILE_NAME` | Specifies the AWS profile to use for authentication. Useful when you have multiple AWS profiles configured. |
| `--bucket BUCKET_NAME` | S3 bucket name (default: ncsh-app-data). |
| `--architecture-version VERSION` | Architecture version to use (default: v2). |

#### Local Simulator Parameters

| Parameter | Description |
| --------- | ----------- |
| `--year YYYY` | Year to process (required). |
| `--month MM` | Month to process (required). |
| `--day DD` | Day to process (required for day mode). |
| `--mode day\|month` | Process a single day or an entire month. Default is day. |
| `--force-scrape` | Forces re-scraping even if data exists locally. |

### Monitoring Execution

You can monitor workflow execution in the AWS Step Functions console or using the AWS CLI:

```bash
# Check execution status
aws stepfunctions describe-execution --execution-arn <execution-arn>

# For recursive workflows with sub-executions, you can also check the status of child executions
# by looking at the parent execution's output and finding the child execution ARNs
```

### Accessing Processed Data

#### Cloud Execution Data
Processed data is available in both JSON and Parquet formats in the S3 bucket:

- JSON files: `s3://ncsh-app-data/v2/processed/json/year=YYYY/month=MM/day=DD/`
- Parquet dataset: `s3://ncsh-app-data/v2/processed/parquet/data.parquet`
- Processing results: `s3://ncsh-app-data/v2/processed/parquet/processing_results/`

#### Local Execution Data
Processed data is available in the local data directory:

- HTML files: `./data/html/`
- JSON files: `./data/json/`

### Performance Considerations

The recursive workflow is designed to handle large date ranges efficiently by:

1. Breaking down large date ranges into manageable batches
2. Processing each batch in parallel where possible
3. Storing detailed processing results in S3 instead of passing them directly in the Step Functions response
4. Using a two-phase approach to decouple scraping from processing

This approach avoids the 256KB payload size limit in AWS Step Functions and allows processing of multi-year date ranges without issues.

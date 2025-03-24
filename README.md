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

### Running the Unified Workflow

The unified workflow processes soccer game data for a specified date range with automatic batching for improved reliability and performance. The workflow handles scraping, validation, and data processing in a single execution.

```bash
# Process data for a specific date range
python scripts/trigger_batched_workflow.py --date-range 2024-01-01 2024-12-31

# Optional: Specify batch size (default is 3 days)
python scripts/trigger_batched_workflow.py --date-range 2024-01-01 2024-12-31 --batch-size 3

# Optional: Force re-scrape of existing data
python scripts/trigger_batched_workflow.py --date-range 2024-01-01 2024-12-31 --force-scrape

# Optional: Use a specific AWS profile
python scripts/trigger_batched_workflow.py --date-range 2024-01-01 2024-12-31 --profile your-profile-name
```

### Parameters Explained

| Parameter | Description |
| --------- | ----------- |
| `--date-range START_DATE END_DATE` | Specifies the start and end dates for data processing in YYYY-MM-DD format. The workflow will process all dates inclusive of both start and end dates. |
| `--batch-size DAYS` | Number of days to include in each batch. The workflow divides the date range into batches to optimize processing. A smaller batch size (e.g., 3-5 days) can improve reliability for problematic date ranges, while a larger batch size (e.g., 7-14 days) is more efficient for stable date ranges. Default is 3 days. |
| `--force-scrape` | Forces the workflow to re-scrape and re-process data even if it already exists in the destination. Without this flag, the workflow skips dates that have already been processed. |
| `--profile PROFILE_NAME` | Specifies the AWS profile to use for authentication. Useful when you have multiple AWS profiles configured. |

### Monitoring Execution

You can monitor the workflow execution in the AWS Step Functions console or using the AWS CLI:

```bash
# Check execution status
aws stepfunctions describe-execution --execution-arn <execution-arn>
```

### Accessing Processed Data

Processed data is available in both JSON and Parquet formats in the S3 bucket:

- JSON files: `s3://ncsh-app-data/v2/processed/json/year=YYYY/month=MM/day=DD/`
- Parquet dataset: `s3://ncsh-app-data/v2/processed/parquet/data.parquet`
- Processing results: `s3://ncsh-app-data/v2/processed/parquet/processing_results/`

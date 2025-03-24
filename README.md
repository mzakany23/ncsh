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

### Running the Workflow

There are two ways to run the soccer data pipeline:

#### 1. Cloud Execution (AWS Step Functions)

This method triggers the AWS Step Functions workflow to run in the cloud:

```bash
# Process data for a specific date range
python scripts/trigger_batched_workflow.py --date-range 2021-01-01 2021-12-31

# Optional: Specify batch size (default is 3 days)
python scripts/trigger_batched_workflow.py --date-range 2021-01-01 2021-12-31 --batch-size 10

# Optional: Force re-scrape of existing data
python scripts/trigger_batched_workflow.py --date-range 2021-01-01 2021-12-31 --force-scrape

# Optional: Use a specific AWS profile
python scripts/trigger_batched_workflow.py --date-range 2021-01-01 2021-12-31 --profile your-profile-name
```

#### 2. Local Execution (Workflow Simulator)

This method simulates the workflow locally without using AWS services:

```bash
# Process data for a specific day
python scripts/local_workflow_simulator.py --year 2021 --month 1 --day 15 --mode day

# Process data for an entire month
python scripts/local_workflow_simulator.py --year 2021 --month 1 --mode month

# Force re-scrape of existing data
python scripts/local_workflow_simulator.py --year 2021 --month 1 --day 15 --force-scrape
```

Local execution stores data in the `./data` directory and execution logs in `./test_output`.

### Parameters Explained

#### Cloud Execution Parameters

| Parameter | Description |
| --------- | ----------- |
| `--date-range START_DATE END_DATE` | Specifies the start and end dates for data processing in YYYY-MM-DD format. The workflow will process all dates inclusive of both start and end dates. |
| `--date YYYY-MM-DD` | Process a single date. |
| `--month YYYY MM` | Process an entire month (e.g., `--month 2021 3` for March 2021). |
| `--batch-size DAYS` | Number of days to include in each batch. The workflow divides the date range into batches to optimize processing. A smaller batch size (e.g., 3-5 days) can improve reliability for problematic date ranges, while a larger batch size (e.g., 7-14 days) is more efficient for stable date ranges. Default is 3 days. |
| `--force-scrape` | Forces the workflow to re-scrape and re-process data even if it already exists in the destination. Without this flag, the workflow skips dates that have already been processed. |
| `--profile PROFILE_NAME` | Specifies the AWS profile to use for authentication. Useful when you have multiple AWS profiles configured. |

#### Local Execution Parameters

| Parameter | Description |
| --------- | ----------- |
| `--year YYYY` | Year to process (required). |
| `--month MM` | Month to process (required). |
| `--day DD` | Day to process (required for day mode). |
| `--mode day\|month` | Process a single day or an entire month. Default is day. |
| `--force-scrape` | Forces re-scraping even if data exists locally. |

### Monitoring Execution

You can monitor cloud workflow execution in the AWS Step Functions console or using the AWS CLI:

```bash
# Check execution status
aws stepfunctions describe-execution --execution-arn <execution-arn>
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

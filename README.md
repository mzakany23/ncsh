# NC Soccer Data Pipeline

A data pipeline for collecting and processing soccer game data.

## Project Structure

```
/
├── scraping/           # Soccer schedule scraping module
│   ├── ncsoccer/      # Scrapy spider and core scraping logic
│   ├── tests/         # Tests for scraping module
│   ├── requirements.txt
│   └── setup.py
├── processing/         # Data processing module
│   ├── lambda_function.py
│   ├── requirements.txt
│   └── Dockerfile
├── utils/             # Utility Lambda functions
│   ├── src/           # Source code for utility functions
│   └── Dockerfile     # Docker build for utility functions
├── terraform/         # Infrastructure as code
│   └── infrastructure/
├── scripts/          # Utility scripts
└── Makefile         # Build and deployment tasks
```

## Setup

1. Install dependencies:
```bash
make install
```

2. Run tests:
```bash
make test
```

3. Deploy infrastructure:
```bash
cd terraform/infrastructure
terraform init
terraform apply
```

## Usage

### Scraping Data

#### Using the Unified Workflow with Batching

The unified workflow allows scraping data for a single day, a date range, or an entire month, with batching for improved reliability:

```bash
# Trigger the unified workflow for a single date
python scripts/trigger_batched_workflow.py --date 2024-03-01

# Trigger for a date range with custom batch size
python scripts/trigger_batched_workflow.py --date-range 2024-03-01 2024-03-31 --batch-size 5

# Trigger for an entire month
python scripts/trigger_batched_workflow.py --month 2024 3

# Force re-scraping of data
python scripts/trigger_batched_workflow.py --date 2024-03-01 --force-scrape

# Use a specific AWS profile
python scripts/trigger_batched_workflow.py --date 2024-03-01 --profile your-profile-name
```

#### Legacy Scraping (previous version)

To scrape a month of data using the legacy workflow:
```bash
make scrape-month YEAR=2024 MONTH=3
```

### Processing Data

To trigger data processing:
```bash
make process-data
```

### Backfill Historical Data

The project includes a backfill mechanism to scrape and process historical data. The backfill step function is configurable to scrape data for specific date ranges.

Deploying the backfill infrastructure:
```bash
make deploy-backfill
```

Running a backfill job:
```bash
make run-backfill
```

Monitoring backfill jobs:
```bash
# Check backfill status
make check-backfill

# Monitor backfill execution in real-time
make monitor-backfill

# Analyze a specific execution (replace with actual ARN)
make analyze-execution execution=arn:aws:states:us-east-2:552336166511:execution:ncsoccer-backfill:backfill-smoke-test-1234567890
```

The backfill process uses an optimized approach:

1. A specialized backfill spider maintains a browser session while navigating through months:
   - Starts at the most recent month
   - Scrapes all days in that month
   - Navigates backward one month (one click)
   - Repeats until reaching the oldest target month

2. Key advantages of this approach:
   - Minimizes navigational overhead (constant number of clicks per month)
   - Uses checkpointing to resume from interruptions
   - Processes months sequentially for efficiency
   - Reuses the browser session to maintain state

3. The backfill can be run:
   - Locally: `make run-local-backfill start_year=2007 start_month=1 end_year=2023 end_month=12`
   - Via AWS Step Function: `make run-backfill`
## Project Notes

### Requirements Management

The project uses a modular approach to managing dependencies:

- **Module-specific requirements**: Each module (`scraping/`, `processing/`) has its own `requirements.in` and/or `requirements.txt` file for module-specific dependencies.

## Development

- Run linting: `make lint`
- Format code: `make format`
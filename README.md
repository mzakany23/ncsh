# NC Soccer Data Pipeline

A data pipeline for collecting and processing soccer game data.

## Project Structure

```
/
├── scraping/           # Soccer schedule scraping module
│   ├── ncsoccer/      # Web scraping logic using requests and BeautifulSoup
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

## Data Architecture

This project uses the v2 data architecture, which provides:

- Organized directory structure for better data management
- Enhanced performance in data processing
- Clear separation of JSON and Parquet data
- Standardized file naming conventions

### Directory Structure

The architecture uses the following S3 directory structure:

```
ncsh-app-data/
└── v2/
    └── processed/
        ├── json/
        │   └── year=YYYY/
        │       └── month=MM/
        │           └── day=DD/
        │               ├── YYYY-MM-DD_games.jsonl  # Game data in JSONL format
        │               └── YYYY-MM-DD_meta.json    # Metadata for the day
        └── parquet/
            ├── YYYY-MM-DD-HH-MM-SS/               # Timestamped snapshots
            │   └── data.parquet
            ├── data.parquet                        # Latest complete dataset
            └── last_processed.json                 # Processing status info
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

The project uses requests and BeautifulSoup for web scraping. The scraping functionality is implemented in the `SimpleScraper` class that provides a clean interface for collecting soccer game data.

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

### Processing Data

To trigger data processing:
```bash
make process-data
```

### Backfill Historical Data

The project includes a backfill mechanism to scrape and process historical data efficiently:

```bash
# Run a backfill job via AWS Step Function
make run-backfill

# Run a backfill job locally with specific date range
make run-local-backfill start_year=2007 start_month=1 end_year=2023 end_month=12
```

Monitoring backfill jobs:
```bash
# Check backfill status
make check-backfill

# Monitor backfill execution in real-time
make monitor-backfill
```

## Project Notes

### Requirements Management

The project uses a modular approach to managing dependencies:

- **Module-specific requirements**: Each module (`scraping/`, `processing/`) has its own `requirements.in` and/or `requirements.txt` file for module-specific dependencies.

## Development

- Run linting: `make lint`
- Format code: `make format`
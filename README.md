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

To scrape a month of data:
```bash
make scrape-month YEAR=2024 MONTH=3
```

### Processing Data

To trigger data processing:
```bash
make process-data
```

## Development

- Run linting: `make lint`
- Run tests: `make test`
- Deploy scraper: `make deploy-scraper`
- Deploy processor: `make deploy-processing`

## Infrastructure

The project uses AWS services:
- Lambda for running scraper and processor
- Step Functions for orchestration
- S3 for data storage
- DynamoDB for metadata
- EventBridge for scheduling
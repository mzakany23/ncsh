# NC Soccer Schedule Scraper

A robust web scraper for collecting North Coast soccer game schedules. Built with Scrapy and deployable to AWS Lambda.

## Features (v1.0.0)

- **Schedule Scraping**
  - Scrape game schedules by day or month
  - Collect detailed game information including teams, locations, and times
  - Support for historical and future game schedules

- **Data Management**
  - JSON output format with schema validation
  - Versioned storage in S3
  - Tracks already scraped dates to prevent duplicates
  - Optimistic locking for concurrent operations

- **Deployment Options**
  - Local execution via Makefile commands
  - Containerized AWS Lambda deployment
  - Automated monthly scraping via EventBridge
  - Step Function workflow for reliable execution

## Local Development

1. Set up Python environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
   pip install -r requirements.txt
   ```

2. Run the scraper:
   ```bash
   # Scrape a specific month
   python runner.py --mode month --year 2024 --month 2

   # Scrape a specific day
   python runner.py --mode day --year 2024 --month 2 --day 1
   ```

## AWS Deployment

The project uses Terraform for infrastructure deployment and GitHub Actions for CI/CD.

Required AWS resources:
- ECR repository for container images
- Lambda function for execution
- S3 bucket for data storage
- Step Function for orchestration
- EventBridge for scheduling

Required GitHub secrets:
- `AWS_ROLE_ARN`: IAM role for GitHub Actions
- `AWS_ACCOUNT_ID`: AWS account ID
- `DATA_BUCKET_NAME`: S3 bucket name for scraped data

## Project Structure

```
├── ncsoccer/              # Main scraper package
│   ├── spiders/           # Scrapy spiders
│   └── pipeline/          # Data processing pipeline
├── terraform/             # Infrastructure as code
├── .github/workflows/     # CI/CD configuration
├── Dockerfile            # Container definition
├── lambda_function.py    # AWS Lambda handler
└── runner.py            # Local execution script
```

## License

MIT License

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
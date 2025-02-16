# NC Soccer Schedule Scraper

A robust web scraper for collecting North Coast soccer game schedules. Built with Scrapy and deployable to AWS Lambda.

## Initial Setup

Before using the automated CI/CD pipeline, you must manually set up the required AWS infrastructure:

1. Configure AWS credentials locally:
   ```bash
   export AWS_PROFILE=<your-profile-name>
   ```

2. Apply the setup module to create bootstrap resources:
   ```bash
   cd terraform/setup
   terraform init
   terraform apply
   ```

   This creates:
   - S3 bucket for Terraform state
   - DynamoDB table for state locking
   - GitHub Actions OIDC provider
   - Base IAM role and policies for GitHub Actions

3. Note the outputs, you'll need them for GitHub configuration:
   - `github_actions_role_arn`: Use this as AWS_ROLE_ARN in GitHub secrets

After these manual steps are complete, the GitHub Actions automation will have the necessary permissions to manage all other infrastructure.

## Features

- **Schedule Scraping**
  - Scrape game schedules by day or month
  - Collect detailed game information including teams, locations, and times
  - Support for historical and future game schedules
  - Asynchronous execution using Twisted's asyncio reactor

- **Data Management**
  - JSON output format with schema validation
  - Versioned storage in S3
  - Tracks already scraped dates to prevent duplicates
  - Optimistic locking for concurrent operations

- **Deployment Options**
  - Local execution via Python scripts
  - Containerized AWS Lambda deployment
  - Automated monthly scraping via EventBridge
  - Step Function workflow for reliable execution

## GitHub Environment Configuration

The project uses GitHub Environments for configuration. Set up the following in your GitHub repository:

### Environment Variables
Set these in your GitHub environment (e.g., "dev"):
- `AWS_REGION`: AWS region for resources (e.g., "us-east-2")
- `ECR_REPOSITORY`: ECR repository name (e.g., "ncsoccer-scraper")
- `TF_STATE_BUCKET`: S3 bucket for Terraform state (e.g., "your-terraform-state")

### Environment Secrets
Set these in your GitHub environment secrets:
- `AWS_ROLE_ARN`: IAM role ARN for GitHub Actions
- `DATA_BUCKET_NAME`: S3 bucket name for scraped data
- `ALERT_EMAIL`: Email address for notifications (optional)

## Local Development

1. Set up Python environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
   pip install -r requirements.txt
   ```

2. Run the scraper locally:
   ```bash
   # Scrape a specific month
   .venv/bin/python runner.py --mode month --year 2024 --month 2

   # Scrape a specific day
   .venv/bin/python runner.py --mode day --year 2024 --month 2 --day 1
   ```

3. Trigger AWS Step Function execution:
   ```bash
   .venv/bin/python ./scripts/trigger_step_function.py \
     --state-machine-arn "arn:aws:states:us-east-2:YOUR_ACCOUNT_ID:stateMachine:YOUR_STATE_MACHINE_NAME" \
     --year 2024 \
     --month 3 \
     --day 1 \
     --mode day \
     --profile YOUR_AWS_PROFILE
   ```

## AWS Deployment

The project uses Terraform for infrastructure deployment and GitHub Actions for CI/CD.

Required AWS resources:
- ECR repository for container images
- Lambda function for execution
- S3 bucket for data storage
- Step Function for orchestration
- EventBridge for scheduling
- DynamoDB for Terraform state locking

The CI/CD pipeline automatically:
1. Builds and tests the application
2. Deploys infrastructure changes
3. Updates the Lambda function with the latest code
4. Configures monitoring and alerting

## Project Structure

```
├── ncsoccer/              # Main scraper package
│   ├── spiders/           # Scrapy spiders
│   └── pipeline/          # Data processing pipeline
├── scripts/               # Utility scripts
│   └── trigger_step_function.py  # AWS Step Function trigger
├── terraform/             # Infrastructure as code
│   ├── setup/            # Bootstrap resources
│   └── infrastructure/   # Main infrastructure
├── tests/                # Test suite
│   └── integration/      # Integration tests
├── .github/workflows/    # CI/CD configuration
├── Dockerfile           # Container definition
├── lambda_function.py   # AWS Lambda handler
└── runner.py           # Local execution script
```

## Testing

Run integration tests:
```bash
.venv/bin/python -m pytest -v tests/integration/
```

The test suite includes:
- Lambda function integration tests
- S3 storage tests
- Step Function execution tests

## License

MIT License

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please ensure to:
1. Update tests as needed
2. Update documentation
3. Follow the existing code style
4. Add meaningful commit messages
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

<<<<<<< HEAD
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

### Refreshing Database

To refresh your local matches.parquet file with the latest version from S3:
```bash
# Using default settings (ncsh-app-data bucket and data/parquet/ prefix)
make refresh-db

# Using a custom bucket
make refresh-db S3_BUCKET=my-custom-bucket

# Using a custom prefix
make refresh-db S3_PREFIX=custom/path/

# Using both custom bucket and prefix
make refresh-db S3_BUCKET=my-custom-bucket S3_PREFIX=custom/path/
```

The command automatically:
1. Creates a backup of your existing database file as `analysis/matches.parquet.bak` before downloading the new version
2. Downloads the data to `analysis/matches.parquet` for use with the CLI interface
3. Creates a copy at `analysis/ui/matches.parquet` for use with the Streamlit UI

This dual-file approach ensures compatibility with both command-line queries and the web interface.

### Analyzing Data

The analysis module provides a smart chat interface to query the soccer data using natural language. The data is stored in Parquet format and queried using DuckDB, with Anthropic's Claude 3.7 Sonnet powering the natural language understanding.

#### Running Queries via Make Command

The simplest way to run queries is using the `query-llama` make command:

```bash
# Set your Anthropic API key (required)
export ANTHROPIC_API_KEY='your-anthropic-api-key-here'

# Ask a question about the data
make query-llama query="Show me the top 5 teams by goals scored"

# Enable verbose mode for more detailed logs
make query-llama query="How did Key West do this month" verbose=true

# Use a specific session ID for conversation continuity
make query-llama query="Show me their next match" session_id=your-session-id
```

#### Using the Python Interface

You can also use the Python interface directly:

```bash
# Set your Anthropic API key (required)
export ANTHROPIC_API_KEY='your-anthropic-api-key-here'

# Set your OpenAI API key (optional, for backward compatibility)
export OPENAI_API_KEY='your-openai-api-key-here'

# Ask questions about the data
python analysis/query_cli.py "Show me all games where Key West scored more than 3 goals"
```

Options:
- `--db`: Path to Parquet file (default: matches.parquet)
- `--model`: Anthropic model to use (default: claude-3-7-sonnet-latest)
- `--session`: Session ID for conversation continuity
- `--verbose`: Enable verbose logging
- `--interactive`: Run in interactive mode

Example Questions:
```bash
# Basic game queries
make query-llama query="What was Key West's highest scoring game?"
make query-llama query="Show me all home games where Key West won by more than 2 goals"
make query-llama query="List all games played in March 2024"

# Statistical analysis
make query-llama query="What is Key West's win-loss record for away games?"
make query-llama query="Show me the teams Key West has played against, ordered by number of matches"

# Complex queries
make query-llama query="Find games where both teams scored at least 2 goals"
make query-llama query="What's the longest winning streak Key West has had?"
make query-llama query="Compare Key West's performance in home vs away games"
```

#### Using the Streamlit UI

The project also includes a Streamlit-based chat interface:

```bash
# Run the Streamlit UI
cd analysis
streamlit run ui/Home.py
```

=======
>>>>>>> origin/main
## Project Notes

### Requirements Management

The project uses a modular approach to managing dependencies:

- **Module-specific requirements**: Each module (`scraping/`, `processing/`) has its own `requirements.in` and/or `requirements.txt` file for module-specific dependencies.

## Development

- Run linting: `make lint`
- Format code: `make format`
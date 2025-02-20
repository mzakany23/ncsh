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
├── analysis/          # Data analysis and chat interface
│   ├── main.py       # OpenAI-powered SQL query interface
│   └── requirements.txt
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

### Analyzing Data

The analysis module provides an OpenAI-powered chat interface to query the soccer data using natural language. The data is stored in Parquet format and queried using DuckDB.

To use the chat interface:
```bash
# Set your OpenAI API key
export OPENAI_API_KEY='your-api-key-here'

# Ask questions about the data
python analysis/main.py -p "Show me all games where Key West scored more than 3 goals"
```

Options:
- `-p, --prompt`: Your question about the data (required)
- `-d, --db`: Path to Parquet file (default: data/parquet/data.parquet)
- `-c, --compute`: Maximum number of agent computation loops (default: 10)

Example Questions:
```bash
# Basic game queries
python analysis/main.py -p "What was Key West's highest scoring game?"
python analysis/main.py -p "Show me all home games where Key West won by more than 2 goals"
python analysis/main.py -p "List all games played in March 2024"

# Statistical analysis
python analysis/main.py -p "What is Key West's win-loss record for away games?"
python analysis/main.py -p "Calculate the average goals scored by Key West in their last 5 games"
python analysis/main.py -p "Show me the teams Key West has played against, ordered by number of matches"

# Complex queries
python analysis/main.py -p "Find games where both teams scored at least 2 goals"
python analysis/main.py -p "What's the longest winning streak Key West has had?"
python analysis/main.py -p "Compare Key West's performance in home vs away games"
```

The chat interface will intelligently:
- Parse your natural language question
- Explore the available data tables and schema
- Generate appropriate SQL queries
- Test the queries for accuracy
- Present the results in a readable format

## Development

- Run linting: `make lint`
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
│   ├── query/        # Refactored query engine modules
│   ├── ui/           # Streamlit UI components
│   ├── query_cli.py  # Command-line interface for queries
│   └── requirements.in
├── terraform/         # Infrastructure as code
│   └── infrastructure/
├── scripts/          # Utility scripts
├── requirements.txt  # Combined dependencies for analysis module
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

## Project Notes

### Requirements Management

The project uses a modular approach to managing dependencies:

- **Module-specific requirements**: Each module (`scraping/`, `processing/`, `analysis/`) has its own `requirements.in` and/or `requirements.txt` file for module-specific dependencies.

- **Root requirements.txt**: The root `requirements.txt` is a compiled version of the analysis module's requirements, generated using `uv pip compile analysis/requirements.in -o requirements.txt`. This compiled file includes all resolved dependencies with pinned versions for reproducibility.

## Development

- Run linting: `make lint`
- Format code: `make format`
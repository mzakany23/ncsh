# NC Soccer Analysis

A Python package for analyzing soccer match data using natural language queries powered by LlamaIndex and Anthropic Claude.

## Features

- Natural language query interface for soccer match data
- Powered by Anthropic's Claude 3.7 model
- Structured data analysis with DuckDB
- Interactive CLI mode for conversation-based queries
- Support for team and division-specific queries

## Installation

```bash
# With uv (recommended)
uv install -e .

# With pip
pip install -e .
```

## Usage

### CLI Mode

```bash
# Run an interactive query session
python -m analysis.query_cli

# Run a specific query
python -m analysis.query_cli "Show me all matches where the home team scored more than 3 goals"

# Run with specific database file
python -m analysis.query_cli --db path/to/matches.parquet "Show standings for division 1"
```

### API Usage

```python
from analysis.query import setup_query_engine, run_query

# Run a query with default settings
response = run_query("Which team has the most wins in division 2?")

# Or set up a query engine for multiple queries
engine = setup_query_engine(
    db_path="matches.parquet",
    model_name="claude-3-7-sonnet-latest",
    verbose=True
)

# Run multiple queries with the same engine
response1 = engine.query("Show me the top scoring teams")
response2 = engine.query("Who has the best defense?")
```

## Package Structure

- `analysis/`
  - `query/`: Core query engine functionality
    - `core/`: Query engine implementation
    - `models/`: Language model interfaces (Claude)
    - `sql/`: Database interfaces (DuckDB)
    - `utils/`: Utility functions
  - `query_cli.py`: Command-line interface

## Requirements

- Python 3.8+
- LlamaIndex 0.9.0+
- Anthropic API key
- DuckDB 0.10.0+

## License

MIT
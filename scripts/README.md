# Scripts

This directory contains utility scripts for managing the NC Soccer data pipeline.

## Workflow Scripts

### trigger_batched_workflow.py

Triggers a batched workflow for processing soccer data for a specific date range.

#### Usage

```bash
python trigger_batched_workflow.py [options]
```

#### Options

- `--start-date`: Start date in YYYY-MM-DD format
- `--end-date`: End date in YYYY-MM-DD format (defaults to start-date if not provided)
- `--force-scrape`: Force re-scraping of data
- `--batch-size`: Number of days to process in each batch
- `--profile`: AWS profile to use
- `--region`: AWS region (default: 'us-east-2')

#### Examples

Process a single date:
```bash
python trigger_batched_workflow.py --start-date 2025-02-15
```

Process a date range:
```bash
python trigger_batched_workflow.py --start-date 2024-11-01 --end-date 2024-11-30
```

## Conversion Scripts

### convert_json_format.py

Converts between different JSON formats for the soccer data.

See the script's help for usage details:
```bash
python convert_json_format.py --help
```
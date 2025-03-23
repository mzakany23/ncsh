# Scripts

This directory contains utility scripts for managing the NC Soccer data pipeline.

## Migration Scripts

### migrate_v1_to_v2.py

This script helps migrate data from the v1 architecture to the v2 architecture structure in AWS S3.

#### Usage

```bash
python migrate_v1_to_v2.py [options]
```

#### Options

- `--bucket`: AWS S3 bucket name (default: 'ncsh-app-data')
- `--profile`: AWS profile to use from your credentials file
- `--region`: AWS region (default: 'us-east-2')
- `--execute`: Execute the migration (without this flag, runs in dry-run mode)

#### Examples

Run a dry run to see what would be migrated:
```bash
python migrate_v1_to_v2.py
```

Execute the migration:
```bash
python migrate_v1_to_v2.py --execute
```

Use a different AWS profile:
```bash
python migrate_v1_to_v2.py --profile my-profile --execute
```

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
- `--architecture`: Data architecture version to use (choices: 'v1', 'v2', default: 'v2')
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
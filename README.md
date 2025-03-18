# NC Soccer Data Processing Pipeline

This project implements a structured data processing pipeline for North Carolina soccer game data. The pipeline processes HTML data from the web, transforms it into structured JSON, and finally converts to Parquet format for analytics.

## Data Structure

The data is organized into the following structure:

### HTML Data
- Path: `s3://ncsh-app-data/data/html/`
- Format: Raw HTML files from the web
- Naming: `YYYY-MM-DD.html` (date of the games)

### JSON Data
- Path: `s3://ncsh-app-data/data/json/`
- Format: Structured JSON files
- Organization: Partitioned directory structure
  ```
  data/json/
    year=YYYY/
      month=MM/
        day=DD/
          data.json
  ```

### Parquet Data
- Path: `s3://ncsh-app-data/data/parquet/`
- Format: Columnar Parquet files
- Files:
  - `ncsoccer_games_YYYY-MM-DD-HH-MM-SS.parquet` (timestamped versions)
  - `ncsoccer_games_latest.parquet` (always points to the latest version)

## Checkpoint System

The pipeline includes a robust checkpoint system that tracks which files have already been processed:

- Each processed file's path is stored in an ordered set (implemented as a dictionary for O(1) lookup)
- Allows for efficient incremental processing
- Supports granular tracking of exactly which files were processed
- Maintains processing history with timestamps and date ranges

Checkpoint files are stored at `s3://ncsh-app-data/data/checkpoints/`:
- `html_processing.json`: Tracks HTML to JSON processing
- `json_to_parquet.json`: Tracks JSON to Parquet processing

## Scripts

### process_html.py
Converts HTML files to structured JSON format:

```
python scripts/process_html.py --bucket ncsh-app-data [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--dry-run]
```

### json_to_parquet.py
Converts JSON files to Parquet format:

```
python scripts/json_to_parquet.py --bucket ncsh-app-data [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--dry-run]
```

### run_pipeline.py
Runs the entire pipeline:

```
python scripts/run_pipeline.py --bucket ncsh-app-data [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--html-only] [--json-only] [--force-reprocess] [--dry-run]
```

### checkpoint.py
Utilities for managing processing checkpoints:

```
python scripts/checkpoint.py --bucket ncsh-app-data [--list] [--initialize]
```

## Features

- **Partitioned Storage**: Data is stored in a partitioned directory structure for efficient querying
- **Incremental Processing**: Process only new files using the checkpoint system
- **Versioned Datasets**: Each processing run creates a timestamped dataset version
- **Latest View**: Always have access to the latest complete dataset via the `latest.parquet` file
- **Efficient Lookups**: O(1) checkpoint lookups for determining if files are already processed
- **Robust Error Handling**: Processing errors are logged and won't stop the pipeline
- **Dry Run Mode**: Preview processing steps without making changes

## Usage Examples

### Initial Processing
```
python scripts/run_pipeline.py --bucket ncsh-app-data
```

### Process a Specific Date Range
```
python scripts/run_pipeline.py --bucket ncsh-app-data --start-date 2022-01-01 --end-date 2022-01-31
```

### Incremental Processing (only processes new files)
```
python scripts/run_pipeline.py --bucket ncsh-app-data
```

### Force Reprocessing of All Files
```
python scripts/run_pipeline.py --bucket ncsh-app-data --force-reprocess
```

### List Processed Files
```
python scripts/checkpoint.py --bucket ncsh-app-data --list
```
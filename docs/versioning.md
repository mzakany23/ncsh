# Dataset Versioning

This document explains how dataset versioning works in the NC Soccer project.

## Overview

The NC Soccer project now supports versioned datasets, allowing you to:
- Track changes over time
- Reference specific historical versions
- Always have access to the latest data
- Run comparison analytics between different versions

## How It Works

### Versioning Format

Datasets are versioned with a datetime-based format:

```
ncsoccer_games_YYYY-MM-DD-HH-MM-SS.parquet
```

For example:
```
ncsoccer_games_2025-03-18-14-30-00.parquet
```

A special `latest` version is always available at:
```
ncsoccer_games_latest.parquet
```

### Infrastructure Support

The AWS infrastructure has been updated to support versioning:

1. **Lambda Functions**: 
   - Now accept an optional `version` parameter
   - When not specified, use the current datetime as the version
   - Maintain both versioned datasets and update the `latest` version

2. **Step Functions**:
   - Pass version information through the entire workflow
   - Support for configurable operations with date ranges
   - Improved error handling and result tracking

3. **IAM Permissions**:
   - Enhanced permissions for S3 operations (ListObjectVersions, CopyObject)
   - Bucket policies configured for versioning operations

## Using Versioned Datasets

### In AWS Step Functions

When starting a Step Function execution, you can specify a version:

```json
{
  "operation": "configurable",
  "src_bucket": "ncsh-app-data",
  "src_prefix": "data/json/",
  "dst_bucket": "ncsh-app-data",
  "dst_prefix": "data/parquet/",
  "version": "2025-03-18",
  "force_full_reprocess": true,
  "date_range": {
    "start_date": "2025-03-01",
    "end_date": "2025-03-18"
  }
}
```

### Accessing Versioned Data

To list available versions:

```bash
aws s3 ls s3://ncsh-app-data/data/parquet/ncsoccer_games_
```

To download a specific version:

```bash
aws s3 cp s3://ncsh-app-data/data/parquet/ncsoccer_games_2025-03-18-14-30-00.parquet ./
```

To always get the latest version:

```bash
aws s3 cp s3://ncsh-app-data/data/parquet/ncsoccer_games_latest.parquet ./
```

## Best Practices

1. For regular reports, reference specific versions to ensure reproducibility
2. For dashboards and live applications, use the `latest` version
3. Include version information in your analysis outputs for traceability
4. When creating critical versions for specific milestones, document them with meaningful descriptions

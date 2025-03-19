# NC Soccer Step Function Workflow - Unified Input Model

## Overview

This document outlines the standardized input model for the NC Soccer unified workflow Step Function. The workflow supports multiple operation modes (daily, monthly, backfill, configurable) through a consistent input structure.

## Workflow Structure

The workflow follows this pattern:
1. **ValidateAndNormalizeInput**: Normalizes all input formats into a consistent structure
2. **DetermineOperationType**: Routes to the appropriate configuration state
3. **Configure*Mode**: Sets mode-specific configuration values
4. **PrepareExecutionParameters**: Routes to the appropriate parameter preparation state
5. **Prepare*Parameters**: Creates standardized input for Lambda functions
6. **Run***: Executes the appropriate Lambda function
7. **ProcessFiles**: Prepares for the file processing phase
8. **ListJSONFiles/ConvertToParquet/BuildFinalDataset**: Processes the data files

## Standardized Input

All operations accept the same input structure with flexible parameter handling. The workflow normalizes values, provides defaults, and organizes parameters into logical groups.

### Input Format

```json
{
  "operation": "daily|monthly|backfill|configurable",
  "parameters": {
    "day": "01",
    "month": "06",
    "year": "2024",
    "start_date": "2024-06-01",
    "end_date": "2024-06-03",
    "startDate": "2024-06-01",  // Legacy support
    "endDate": "2024-06-03",    // Legacy support
    "specific_dates": ["2024-06-01", "2024-06-02", "2024-06-03"],
    "force_scrape": true,
    "useNewProcessingCode": true,
  },
  "date_range": {  // Legacy/Alternative format
    "start_date": "2024-06-01",
    "end_date": "2024-06-03"
  },
  "specific_dates": ["2024-06-01", "2024-06-02", "2024-06-03"],  // Legacy/Alternative format
  "force_full_reprocess": true,
  "src_bucket": "ncsh-app-data",
  "src_prefix": "data/json/",
  "dst_bucket": "ncsh-app-data",
  "dst_prefix": "data/parquet/",
  "version": "latest"
}
```

### Input Normalization

The workflow handles multiple input formats through normalization:
- Support for both `.parameters.start_date` and `.date_range.start_date`
- Support for legacy parameter names (`startDate` vs `start_date`)
- Default values for all parameters
- Consistent organization of storage parameters

## Operation Modes

### Daily Mode
```json
{
  "operation": "daily",
  "parameters": {
    "day": "19",
    "month": "03",
    "year": "2025",
    "force_scrape": true
  }
}
```

### Monthly Mode
```json
{
  "operation": "monthly",
  "parameters": {
    "month": "03",
    "year": "2025",
    "force_scrape": true
  }
}
```

### Backfill Mode
```json
{
  "operation": "backfill",
  "parameters": {
    "start_date": "2024-01-01",
    "end_date": "2024-06-01",
    "useNewProcessingCode": true
  }
}
```

### Configurable Mode
```json
{
  "operation": "configurable",
  "parameters": {
    "specific_dates": ["2024-06-07", "2024-06-08", "2024-06-09"],
    "force_scrape": true
  },
  "force_full_reprocess": true
}
```

## Benefits of the Unified Approach

1. **Consistency**: All operations follow the same input pattern
2. **Flexibility**: Support for multiple parameter naming conventions
3. **Resilience**: Default values and normalization prevent common failures
4. **Maintainability**: Single place for parameter handling logic
5. **Debugging**: Clear parameter flow throughout the entire workflow

## Implementation Notes

- The `ValidateAndNormalizeInput` state uses `States.JsonMerge()` to handle missing values and provide defaults
- Storage parameters are consolidated in a `storage` object for clarity
- Each operation mode has a dedicated parameter preparation path
- All paths converge to a common file processing workflow
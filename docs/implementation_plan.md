# Implementation Plan: Unified Date Range Workflow with Batching

## Overview

This document outlines the complete implementation plan for the unified date range workflow with batching. The goal is to replace the current separate day and month modes with a single, more efficient and reliable workflow that can handle any date range.

## Changes Implemented

### 1. New Lambda Functions

- **Input Validator** (`input-validator`)
  - Located in: `utils/src/input_validator.py`
  - Purpose: Validates and normalizes input parameters
  - Main functionality:
    - Validates date formats
    - Ensures start_date ≤ end_date
    - Normalizes input parameters

- **Batch Planner** (`batch-planner`)
  - Located in: `utils/src/batch_planner.py`
  - Purpose: Divides the date range into manageable batches
  - Main functionality:
    - Calculates number of days in range
    - Divides range into batches of specified size
    - Generates execution plan

- **Batch Verifier** (`batch-verifier`)
  - Located in: `utils/src/batch_verifier.py`
  - Purpose: Verifies batch results and aggregates metrics
  - Main functionality:
    - Aggregates batch results
    - Validates data consistency
    - Generates execution summary

### 2. Infrastructure Changes

- **New Terraform Configuration**
  - Located in: `terraform/infrastructure/lambda-batched.tf`
  - Purpose: Defines the new Lambda functions and IAM roles

- **Step Function Definition**
  - Located in: `terraform/infrastructure/unified-workflow-batched.asl.json`
  - Purpose: Defines the Step Function that orchestrates the workflow
  - Main components:
    - Input validation state
    - Batch planning state
    - Map state for parallel execution
    - Batch verification state

### 3. Scraper Modifications

- **Lambda Function Updates**
  - Updated `scraping/lambda_function.py` to handle batched date range operations
  - Added support for unified input format

### 4. Client Interface

- **Trigger Script**
  - Created `scripts/trigger_batched_workflow.py`
  - Provides easy access to the new workflow
  - Supports various input methods (single date, date range, full month)

### 5. Documentation

- **Unified Workflow Documentation**
  - Created `docs/unified_workflow.md`
  - Comprehensive documentation of the new workflow

- **README Updates**
  - Updated README.md with examples of using the new workflow

- **CHANGELOG Updates**
  - Added version 2.14.0 entry with details of the changes

## Implementation Details

### Step Function Flow

1. **Input Validation**
   ```
   Input → InputValidator → BatchPlanner → Map State → BatchVerifier → Output
                                            ↓
                                     Scraper Lambda
   ```

2. **Map State Structure**
   ```
   Map State
   ├── Batch 1: [2024-03-01, 2024-03-02, 2024-03-03]
   ├── Batch 2: [2024-03-04, 2024-03-05, 2024-03-06]
   └── Batch 3: [2024-03-07, 2024-03-08, 2024-03-09]
   ```

### Execution Example

For a date range of 2024-03-01 to 2024-03-09 with batch_size=3:

1. **Input**:
   ```json
   {
     "start_date": "2024-03-01",
     "end_date": "2024-03-09",
     "batch_size": 3
   }
   ```

2. **After InputValidator**:
   ```json
   {
     "start_date": "2024-03-01",
     "end_date": "2024-03-09",
     "batch_size": 3,
     "force_scrape": false,
     "architecture_version": "v1",
     "bucket_name": "ncsh-app-data",
     "validated": true
   }
   ```

3. **After BatchPlanner**:
   ```json
   {
     "batches": [
       { "dates": ["2024-03-01", "2024-03-02", "2024-03-03"] },
       { "dates": ["2024-03-04", "2024-03-05", "2024-03-06"] },
       { "dates": ["2024-03-07", "2024-03-08", "2024-03-09"] }
     ],
     "metadata": {
       "total_days": 9,
       "batch_count": 3,
       "batch_size": 3
     },
     "original_input": {
       "start_date": "2024-03-01",
       "end_date": "2024-03-09",
       "batch_size": 3,
       "force_scrape": false,
       "architecture_version": "v1",
       "bucket_name": "ncsh-app-data"
     }
   }
   ```

4. **Map State Execution**:
   - Each batch is processed in parallel
   - Each date within a batch is processed sequentially

5. **After BatchVerifier**:
   ```json
   {
     "results": {
       "total_days": 9,
       "successful_days": 9,
       "failed_days": 0,
       "success_rate": 100,
       "execution_time_seconds": 120,
       "batches": [
         {
           "batch_id": 1,
           "status": "success",
           "dates": ["2024-03-01", "2024-03-02", "2024-03-03"]
         },
         {
           "batch_id": 2,
           "status": "success",
           "dates": ["2024-03-04", "2024-03-05", "2024-03-06"]
         },
         {
           "batch_id": 3,
           "status": "success",
           "dates": ["2024-03-07", "2024-03-08", "2024-03-09"]
         }
       ]
     }
   }
   ```

## Deployment Steps

1. **Create Branch**
   - Created branch `unified-date-range-batching`

2. **Local Development and Testing**
   - Implemented all components
   - Tested locally with synthetic data

3. **Infrastructure Deployment**
   - Apply Terraform changes to create new Lambda functions and Step Function
   ```bash
   cd terraform/infrastructure
   terraform apply
   ```

4. **Test in Development Environment**
   - Use trigger script to test workflow
   ```bash
   python scripts/trigger_batched_workflow.py --date-range 2024-03-01 2024-03-03
   ```

5. **Production Deployment**
   - Merge to main branch
   - Update CHANGELOG.md
   - Wait for CI/CD to deploy changes

## Advantages of New Design

1. **Conceptual simplicity**: One unified mode instead of three separate modes
2. **Improved reliability**: Batching prevents Lambda timeouts
3. **Better error handling**: More granular retry mechanisms
4. **Enhanced monitoring**: Detailed metrics per execution
5. **Scalable architecture**: Process large date ranges efficiently

## Migration Path for Existing Users

1. **Documentation**: Updated README and added new documentation
2. **Backward Compatibility**: Legacy commands still work
3. **Simplified Interface**: New interface is easier to understand and use
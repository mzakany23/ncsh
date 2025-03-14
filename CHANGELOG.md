# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.6.0] - 2025-03-14
### Added
- Created unified Step Function workflow to handle all scraping and processing scenarios (daily, monthly, backfill)
- Added new operations to the processing Lambda: build_dataset, check_backfill_status, and process_all
- Improved error handling and retry logic in the Step Function workflow

### Changed
- Reorganized project structure for better maintainability
- Moved test files to a dedicated tests directory
- Added infrastructure as code for the unified workflow in terraform/infrastructure

## [2.5.8] - 2025-03-14
### Fixed
- Fixed bootstrap script in scraper Lambda Dockerfile to properly handle Lambda runtime arguments
- Modified script to pass arguments to the awslambdaric module using "$@"

## [2.5.7] - 2025-03-14
### Fixed
- Implemented more robust error handling for Twisted reactor in backfill_runner.py
- Added multiple fallback mechanisms to ensure backfill process can proceed even without reactor

## [2.5.6] - 2025-03-13
### Fixed
- Added fallback reactor import in backfill_runner.py to prevent 'reactor not defined' error

## [2.5.5] - 2025-03-13
### Fixed
- Fixed datetime comparison error in processing Lambda function
- Resolved issue with timezone-aware and timezone-naive datetime objects

## [2.5.4] - 2025-03-13
### Fixed
- Moved fixed backfill_runner.py to the root directory for Docker build

## [2.5.3] - 2025-03-13
### Fixed
- Updated log file path to use /tmp directory in Lambda environment
- Fixed file permission issues in read-only Lambda file system

## [2.5.2] - 2025-03-13
### Fixed
- Fixed Lambda container bootstrap script formatting to resolve line break issues
- Properly configured bootstrap script in the correct Lambda runtime location
- Removed custom entrypoint for better AWS Lambda compatibility

## [2.5.1] - 2025-03-13
### Fixed
- Fixed Lambda container configuration to resolve the bootstrap entry point issue
- Updated ECR repository permissions for proper Lambda function access
- Improved error handling in containerized Lambda functions

## [1.2.1] - 2024-03-11
### Fixed
- Critical fix to processing Lambda to only process files that are new or modified since the last run
- Added timestamp tracking system to avoid reprocessing old data
- Added ability to force full reprocessing with explicit flag

## [1.2.0] - 2024-03-11
### Changed
- Enhanced data processing to append new data to existing dataset instead of overwriting
- Improved deduplication logic to prevent duplicate entries when processing daily data
- Added better logging for data processing operations
- Updated processing Lambda to track both new and total record counts

## [2.5.0] - 2025-03-13
### Added
- Enhanced score extraction with Claude-powered AI agent as fallback
- Added comprehensive HTML analysis to extract scores from historical data
- Improved support for 2010-era HTML structure in schedule spider

### Changed
- Simplified Step Function workflow to focus on synchronous queries
- Removed WebSocket API Gateway and related resources
- Updated Lambda function environment variables and permissions

### Fixed
- Fixed OpenAI API key handling in Lambda function
- Improved error handling and validation in analysis Lambda
- Added proper result formatting in Step Function responses
- Fixed JSON serialization in Step Function state machine

### Added
- New query_analysis.py script for testing analysis queries
- IAM policy for Step Function access from local development
- Added historical data backfill step function for automated data ingestion
- Implemented robust backfill monitoring and troubleshooting tools
- Added comprehensive logs and metrics for backfill jobs
- Created make commands for running and managing backfill operations

## [2.4.0] - 2025-02-21
### Added
- Removed verbose logging from analysis
- Made analysis more efficient and better fuzzy matching

## [2.3.0] - 2025-02-20
### Fixed
- Fixed analysis Lambda function creation in Terraform
- Fixed Docker build issues with requirements compilation
- Added terraform.tfvars.example for required variables

## [2.2.3] - 2025-02-20
### Added
- Analysis Lambda function for querying soccer data using natural language
- OpenAI-powered SQL query generation for data analysis
- WebSocket API for real-time query responses
- Step Functions workflow for handling analysis requests
- Support for both synchronous and streaming query modes
- Team performance analysis and statistics functions
- DuckDB integration for efficient Parquet file querying
- Rich formatting options for query results

### Changed
- Updated deployment workflow to build and push analysis container
- Consolidated configuration values into single AWS Secrets Manager secret
- Enhanced infrastructure with WebSocket API Gateway setup

## [2.1.5] - 2025-02-19
### Added
- Strict schema validation for game data using Pydantic
- Field-level validation for team names, scores, and URLs
- Detailed validation error tracking and reporting
- Automatic backup of previous Parquet file

### Changed
- Simplified Parquet storage to use single file with backup
- Enhanced data validation with strict type checking
- Improved error handling and logging
- Updated schema enforcement in Parquet generation
- Rebuilt Lambda container with required dependencies

### Fixed
- Prevented empty or whitespace-only strings in critical fields
- Ensured consistent data types in Parquet schema
- Added validation for score values and URL format
- Fixed missing Pydantic dependency in Lambda container

## [2.1.0] - 2025-02-19
### Added
- Pydantic models for game data schema validation
- Strict type checking for Parquet file generation
- Improved error handling for data validation
- Colocate Parquet file to overwrite existing file on new processing run

### Changed
- Refactored JSON to Parquet conversion to use schema validation
- Flattened nested game data structure in Parquet files
- Updated processing Lambda to handle data validation

## [2.0.4] - 2025-02-19
### Changed
- Enhanced Step Function workflow with visual state tracking
- Split processing Lambda into distinct operations for better monitoring
- Added detailed logging for JSON file discovery and Parquet conversion
- Improved error handling and state management in processing pipeline

## [2.0.3] - 2025-02-19
### Changed
- Switched to uv for Python package management
- Improved dependency management with compiled requirements.txt
- Reorganized scraping module into proper Python package structure
- Updated Makefile to handle virtual environments consistently
- Simplified processing module to remove unnecessary package structure

### Fixed
- Fixed Docker build paths in scraping module
- Made build jobs parallel in CI pipeline
- Added back CHANGELOG.md validation step in deployment workflow
- Added check to ensure new version entry exists in CHANGELOG.md
- Fixed scraping module imports by moving runner.py into package
- Updated tests to use correct module paths
- Fixed Lambda function imports to use proper package structure
- Fixed deployment workflow to only trigger on merged PRs to main

## [2.0.1] - 2025-02-16
### Fixed
- Added back CHANGELOG.md validation step in deployment workflow
- Added check to ensure new version entry exists in CHANGELOG.md

## [2.0.0] - 2025-02-16
### Changed
- Simplified deployment process to only trigger on CHANGELOG.md updates
- Added comprehensive smoke test documentation for data verification
- Improved data quality checks with expected patterns for HTML sizes and game counts

### Fixed
- Updated Lambda handler name in Dockerfile to match function name in lambda_function.py

## [1.2.0] - 2025-02-16

### Changed
- Improved integration tests to use test prefixes in S3 and test tables in DynamoDB
- Updated lambda function to handle test environments properly
- Fixed region handling in AWS service clients

## [1.1.0] - 2025-02-16

### Changed
- Removed virtualenv from Docker container in favor of global dependency installation
- Split requirements into prod and dev dependencies
- Simplified CI/CD pipeline by removing infrastructure deployment
- Added change detection to CI/CD pipeline
- Added CHANGELOG.md trigger for automated builds

## [1.0.0] - 2025-02-14

### Added
- Initial release of NC Soccer schedule scraper
- Implemented schedule spider to scrape game data from NC Soccer website
- Added support for scraping by day and month
- Implemented data pipeline for HTML to JSON conversion
- Added JSON validation for scraped data
- Created runner script with flexible date range options
- Basic error handling and logging
- Support for tracking already scraped dates
- Configurable scraping parameters (delay, concurrent requests)

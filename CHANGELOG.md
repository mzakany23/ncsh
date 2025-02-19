# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.1.3] - 2025-02-19
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

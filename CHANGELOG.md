# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.2] - 2025-02-18
### Changed
- Switched to uv for Python package management
- Improved dependency management with compiled requirements.txt
- Reorganized scraping module into proper Python package structure
- Updated Makefile to handle virtual environments consistently

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

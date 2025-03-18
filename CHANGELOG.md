# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.6.17] - 2025-03-18
### Changed
- Unified spider functionality by consolidating BackfillSpider features into ScheduleSpider
- Added flexible date parameters to support both single date and date range scraping
- Improved code maintainability by removing duplicate functionality across spiders
- Added better parameter documentation and more descriptive logging for all operating modes

### Fixed
- Added missing time module import in ScheduleSpider
- Updated HTML parser tests to handle the current HTML structure correctly

### Removed
- Removed BackfillSpider as its functionality is now in the unified ScheduleSpider
- Updated tests to use the ScheduleSpider with the new date range mode

## [2.6.16] - 2025-03-18
### Added
- Implemented direct URL access method using print.aspx endpoint for more reliable data retrieval
- Added get_direct_date_url function to generate printer-friendly URLs with query parameters
- Significantly improved reliability of historical data scraping, especially for dates before 2018
- Completely refactored BackfillSpider to eliminate UI navigation, making it faster and more reliable

## [2.6.15] - 2025-03-18
### Fixed
- Fixed Step Function state machine path reference to correctly access Lambda response payload

## [2.6.14] - 2025-03-18
### Fixed
- Fixed local variable 'time' reference error in runner.py by removing redundant imports that created variable scope issues

## [2.6.13] - 2025-03-17
### Fixed
- Fixed Twisted reactor import error in backfill runner by adding proper fallback handling
- Added defensive coding to handle missing reactor in AWS Lambda environment

## [2.6.12] - 2025-03-17
### Fixed
- Updated CI/CD workflow to explicitly use Docker buildx for cross-platform image building
- Ensured proper x86_64 architecture compatibility for AWS Lambda function images
- Fixed AWS Lambda architecture mismatch during containerized function deployment

## [2.6.11] - 2025-03-17
### Fixed
- Resolved "exec format error" by explicitly targeting x86_64 architecture
- Added architecture-specific base image tag (3.11-x86_64) for Lambda compatibility
- Ensured proper CMD format with double quotes for handler specification
- Fixed Docker image building for cross-architecture deployment from ARM64 Macs

## [2.6.10] - 2025-03-17
### Fixed
- Explicitly set Docker image platform to linux/amd64 to match AWS Lambda architecture
- Eliminated bootstrap script completely to avoid exec format errors
- Simplified container configuration to standard AWS Lambda pattern

## [2.6.9] - 2025-03-17
### Fixed
- Implemented unified Lambda handler approach with a single entrypoint
- Simplified container configuration by using a single handler that handles both standard and backfill modes
- Removed conditional logic from Docker, moving it to the application code for better maintainability

## [2.6.5] - 2025-03-17
### Fixed
- Added "tabulate" Python package to dependencies in environment

## [2.6.4] - 2025-03-14
### Fixed
- Emergency fix for CI/CD with correct version

## [2.6.3] - 2025-03-14
### Fixed
- Lambda backfill process crash during long-running operations 
- Added integration with DynamoDB for backfill status tracking
- Improved error handling for edge cases

## [2.6.2] - 2025-03-13
### Added
- Lambda backfill process now respects historical season boundaries
- Improved retry logic for intermittent failures 
- Added fallback mode for parsing edge cases

## [2.6.1] - 2025-03-13
### Fixed
- Fixed IAM role permissions for Lambda S3 access
- Updated AWS API endpoint handling

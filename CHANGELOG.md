# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.10.4] - 2025-03-19
### Fixed
- Fixed timezone handling issues in Parquet conversion process
- Added consistent handling of datetime objects throughout the processing pipeline
- Updated data models to properly handle timezone information
- Added robust error handling for Parquet conversion
- Added fallback mechanism for Parquet schema issues

## [2.10.3] - 2025-03-19
### Fixed
- Fixed string formatting issue in scraper Lambda function that caused "Unknown format code 'd' for object of type 'str'" error
- Added proper type conversion for day, month, and year parameters in lambda_handler to ensure they're passed as integers
- Added unit tests to verify proper handling of string parameters from Step Function input

## [2.10.2] - 2025-03-19
### Fixed
- Fixed date conversion issue in processing Lambda that caused workflow failures
- Enhanced date parsing in GameData model to handle multiple date formats
- Added robust error handling for date conversion in the build_dataset function
- Improved PyArrow schema definition to better handle nullable timestamps

## [2.10.1] - 2025-03-18
### Fixed
- Fixed format string error in runner.py when handling string parameters
- Fixed parameter passing in unified workflow to maintain parameters between states
- Added proper handling for optional specific_dates parameter in Step Function
- Updated lambda functions to handle string parameters consistently

## [2.10.0] - 2025-03-18
### Added
- Implemented full versioning support for datasets with datetime-based identifiers
- Added test inputs for versioning configuration in unified workflow
- Enhanced Lambda function to support version parameter in convert_to_parquet

### Changed
- Applied Terraform changes to consolidate all workflows into unified state machine
- Updated CloudWatch logging level to ALL for improved debugging
- Enhanced error handling and reporting for versioned dataset operations

## [2.9.1] - 2025-03-18
### Fixed
- Removed lingering references to old step function state machines in terraform
- Updated terraform to consistently use the unified workflow for all operations
- Fixed references to IAM roles in EventBridge targets
- Ensured consistent CloudWatch alarm configuration for the unified workflow

### Changed
- Organized test files into appropriate directories for better repository structure
- Added documentation for test files and examples
- Improved overall code organization and project structure

## [2.9.0] - 2025-03-18
### Changed
- Consolidated all Step Functions (processing, backfill) into a single unified workflow
- Removed unused Step Function state machines that were replaced by the unified workflow
- Updated EventBridge triggers to use the unified workflow with appropriate parameters
- Updated CloudWatch logs configuration to ALL level for better debugging visibility

## [2.8.0] - 2025-03-18
### Added
- Added environment variables to processing Lambda for versioning configuration
- Enhanced IAM permissions for S3 operations to support versioning (ListObjectVersions, CopyObject)
- Improved state machine definition with better error handling and workflow control

## [2.7.0] - 2025-03-18
### Added
- AWS Step Function pipeline for configurable scraping and dataset building
- Versioned parquet datasets with datetime-based versioning in filenames
- Enhanced Lambda functions for scraping and data processing
- AWS deployment configuration with IAM policies and documentation

### Changed
- Added new 'configurable' operation type to the Step Function workflow
- Implemented datetime-based versioning for parquet datasets for better data lineage
- Modified processing Lambda to maintain both versioned and 'latest' datasets
- Updated Lambda to handle specific dates list for targeted scraping operations
- Enhanced error handling and reporting in the Step Function workflow

### Technical Details
- Versioned datasets are now stored with format: `ncsoccer_games_YYYY-MM-DD-HH-MM-SS.parquet/csv`
- 'Latest' datasets are always accessible with consistent path: `ncsoccer_games_latest.parquet/csv`
- Step Function now passes version identifier throughout the workflow for consistent versioning
- Infrastructure deployed with Terraform and Docker containers built via GitHub Actions

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

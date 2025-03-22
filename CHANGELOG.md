# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.14.1] - 2025-03-25
### Fixed
- Fixed Step Function input validation to handle default values properly
- Removed problematic `States.JsonMerge` calls that were causing input validation failures
- Simplified input parameter handling to rely on Lambda's default value capability
- Enabled support for both single-day and date-range operations using the batched workflow

## [2.14.0] - 2025-03-24
### Added
- Implemented unified date range workflow with batching to prevent Lambda timeouts
- Created new utility Lambda functions for input validation, batch planning, and result verification
- Added `utils` module with specialized Lambda functions for workflow management
- Developed a unified trigger script (`trigger_batched_workflow.py`) for easy workflow invocation
- Added comprehensive documentation for the new workflow in `docs/unified_workflow.md`
- Created post-migration task list in `docs/post_migration_tasks.md` to track cleanup activities

### Changed
- Restructured Step Function to use a Map state for parallel batch processing
- Enhanced error handling with batch-level retries and comprehensive error reporting
- Updated deployment workflow to build and deploy utility Lambda functions
- Simplified client interface with a single unified date range approach instead of separate modes
- Deployed new workflow alongside existing workflow to enable gradual migration

### Fixed
- Resolved Lambda timeout issues for monthly operations by implementing batching
- Improved reliability of multi-day scraping through parallel batch processing
- Enhanced error handling to prevent complete workflow failure when individual dates fail

### Notes
- The original workflow (`ncsoccer-unified-workflow`) will be maintained temporarily during the migration period and removed after the new batched workflow is proven stable in production

## [2.13.5] - 2025-03-23
### Changed
- Enhanced Lambda function to strictly enforce S3 storage by removing any file system operations
- Updated `FileStorage` class to provide stronger warnings when used in Lambda environments
- Modified `get_storage_interface` function to automatically enforce S3 storage in Lambda
- Added detailed warnings in all runner functions to prevent file system usage in Lambda
- Improved error messaging to highlight Lambda best practices for storage

## [2.13.4] - 2025-03-22
### Fixed
- Resolved "Read-only file system" errors in Lambda function by ensuring all file operations use the /tmp directory
- Updated FileStorage class to automatically detect Lambda environments and redirect file operations to /tmp
- Modified checkpoint system to properly handle Lambda environments
- Ensured all Lambda operations default to S3 storage to prevent filesystem issues

## [2.13.3] - 2025-03-21
### Fixed
- Added missing `get_direct_date_url` method to `ScheduleSpider` class that was causing scraper failures
- Fixed `DataPathManager` initialization to properly use custom HTML prefix instead of using fixed paths
- Improved path handling for file storage in both local and S3 environments

## [2.13.2] - 2025-03-20
### Fixed
- Increased S3 file creation timeout from 120 to 300 seconds to address Lambda timeout issues in v2 architecture
- Added detailed logging to help diagnose S3 write and verification issues
- Included script for local Lambda testing to reproduce production issues

## [2.13.1] - 2025-03-20
### Fixed
- Fixed incorrect spider name reference in runner.py from 'schedule_spider' to 'schedule'

## [2.13.0] - 2025-03-20
### Added
- Implemented v2 data architecture with improved partitioning and organization
- Created DataPathManager class to standardize S3 path construction
- Added unified checkpoint file system for more efficient date tracking
- Added architecture_version parameter to Lambda function and Step Function interfaces
- Extended trigger_step_function.py script to support architecture version selection

### Changed
- Refactored S3 storage paths to follow a more structured partitioning scheme
- Updated pipeline components to support both v1 and v2 architectures for backward compatibility
- Modified schedule_spider.py to use the new path construction methodology
- Improved error handling and logging around file operations

### Fixed
- Standardized path resolution across all components
- Enhanced checkpoint lookup system for more accurate scraping decisions

## [2.12.1] - 2025-03-20
### Fixed
- Updated CI/CD pipeline to remove references to the now non-existent backfill Lambda function
- Aligned deployment workflow with the unified architecture that uses date_range mode

## [2.12.0] - 2025-03-20
### Changed
- Replaced backfill runner with date_range mode in unified workflow
- Updated Lambda function to handle date ranges directly
- Modified Step Function definition to support date_range operations
- Updated Terraform configuration for new workflow architecture
- Improved unit tests for all operation modes

### Removed
- Removed backfill_runner.py as it is now redundant
- Removed backfill operations from unified workflow Step Function

### Fixed
- Fixed issue with test runner finding Scrapy spiders
- Fixed Lambda function tests to match new parameter structure
- Improved error handling for date parsing and validation

## [2.11.0] - 2025-03-19
### Changed
- Completely refactored Step Function workflow to use a standardized input approach
- Added `ValidateAndNormalizeInput` state to normalize all input formats into a consistent structure
- Consolidated parameter handling to reduce redundancy and potential errors
- Standardized storage parameters to ensure consistent bucket/prefix handling
- Created unified execution flow with proper branch handling
- Simplified maintenance with consistent parameter access patterns

### Fixed
- Fixed operation flow inconsistencies across different operation modes
- Fixed parameter preservation issues that caused failures in backfill operations
- Resolved path inconsistencies between scraping and processing Lambda functions
- Added fallback defaults for all parameters to make input more robust

## [2.10.7] - 2025-03-19
### Fixed
- Fixed MergeBackfillParameters state to preserve S3 bucket and prefix information
- Ensured consistent parameter handling in backfill workflow path
- Corrected parameter path for backfill operations in unified workflow

## [2.10.6] - 2025-03-19
### Fixed
- Fixed backfill operation in unified workflow by adding parameter preservation between states
- Added PreserveParametersAfterBackfill state to maintain context for subsequent Lambda invocations
- Ensured consistent parameter handling across all operation modes (daily, monthly, backfill, configurable)

## [2.10.5] - 2025-03-19
### Fixed
- Fixed timestamp handling in build_dataset function when creating final Parquet files
- Added more robust error handling and fallback mechanisms for Parquet conversion
- Improved schema definition to properly handle timestamp objects
- Added JSON-based intermediate conversion for problematic timestamp fields

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

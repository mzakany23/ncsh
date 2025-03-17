# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

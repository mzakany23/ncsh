# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-02-14

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
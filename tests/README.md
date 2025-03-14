# NCSoccer Testing

This directory contains all the tests for the NCSoccer project, organized by type and functionality.

## Directory Structure

- `unit/`: Unit tests for individual components
  - `processing/`: Tests for processing module components
  - `scraping/`: Tests for scraping module components
- `integration/`: Integration tests that verify component interactions
  - `processing/`: Tests for processing module integrations
  - `scraping/`: Tests for scraping module integrations  
- `functional/`: Functional tests that verify end-to-end functionality

## Running Tests

### Unit Tests

To run all unit tests:

```bash
python -m pytest tests/unit
```

To run specific unit tests:

```bash
python -m pytest tests/unit/scraping
python -m pytest tests/unit/processing
```

### Integration Tests

To run all integration tests:

```bash
python -m pytest tests/integration
```

### Functional Tests

To run the scraper locally for testing:

```bash
python -m tests.functional.test_scraper_local --year 2025 --month 3 --day 14
```

To run the backfill functionality locally:

```bash
python -m tests.functional.test_backfill
```

## Adding New Tests

When adding new tests:

1. Place them in the appropriate directory based on test type (unit, integration, functional)
2. Use the naming convention `test_*.py` for test files
3. Within test files, name test functions with the prefix `test_`
4. Add a docstring to each test function explaining what it tests

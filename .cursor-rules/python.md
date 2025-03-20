python

Python handling

# Python Development Rules

## Package Management

We use `uv` for Python package management (see build rule for details).

### Virtual Environment

Create and activate a virtual environment using Makefile:
```bash
make install  # Creates venv and installs dependencies
```

### Dependencies

- `requirements.in` files define direct dependencies
- `requirements.txt` files are compiled from .in files using uv
- Use Makefile targets for all dependency management:
  - `make compile-requirements`: Update requirements.txt files
  - `make install`: Install all dependencies
  - `make test`: Run tests with all required dependencies

### Project Structure

- Use `pyproject.toml` for package configuration
- Keep dependencies in `requirements.in` files
- Place tests in a `tests` directory
- Use `pytest` for testing

## Code Style

- Follow PEP 8
- Use `ruff` for linting and formatting
- Maximum line length: 100 characters
- Use type hints for function arguments and return values
- Run `make lint` and `make format` before committing
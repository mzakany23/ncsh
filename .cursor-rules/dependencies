dependencies

Dependency management rules

# Dependency Management Rules

## Core Principles
- Use `requirements.in` for direct dependencies
- Use `requirements.txt` (compiled from .in) for pinned dependencies
- Use `pyproject.toml` for development dependencies and project metadata

## File Structure
- Each module has its own `requirements.in` file
- Generated `requirements.txt` files are used for installation
- Development dependencies are specified in `pyproject.toml` under `[project.optional-dependencies]`

## Workflow
1. Add direct dependencies to `requirements.in`
2. Use `make compile-requirements` to generate `requirements.txt`
3. Add development dependencies to `pyproject.toml`
4. Use `make install` to install all dependencies

## Best Practices
- Keep `requirements.in` files minimal with only direct dependencies
- Use version constraints in `requirements.in` only when necessary
- Pin all versions in generated `requirements.txt`
- Group development dependencies logically in `pyproject.toml`
# Quackpipe v0.3.0

A configuration-driven and programmatic ETL helper for DuckDB.

## Project Structure

```
.
├── pyproject.toml              # Project configuration
├── src/quackpipe/             # Main package
│   ├── __init__.py            # Public API exports
│   ├── config.py              # Typed configuration objects
│   ├── secrets.py             # Secret management system
│   ├── builder.py             # Programmatic builder API
│   ├── utils.py               # ETL utility functions
│   ├── core.py                # Core session management
│   ├── exceptions.py          # Custom exceptions
│   └── sources/               # Source handlers
│       ├── __init__.py
│       ├── base.py            # Base handler class
│       ├── postgres.py        # PostgreSQL handler
│       └── s3.py              # S3 handler
├── examples/                  # Usage examples
│   ├── config.yml             # Configuration example
│   └── run_etl_v3.py          # Example script
└── output/                    # Output directory
```

## Features

- Configuration-driven setup via YAML files
- Programmatic builder API for dynamic configurations
- Secret management with multiple provider support
- Auto-generated views for database tables
- ETL utility functions
- Support for PostgreSQL and S3 data sources

## Usage

See `examples/` for usage examples.

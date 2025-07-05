# Quackpipe

**A configuration-driven and programmatic ETL helper for DuckDB.**

Quackpipe simplifies connecting to and moving data between various sources by leveraging DuckDB's powerful extension ecosystem. It allows you to define your data sources in a simple YAML file or build them programmatically, and then provides a clean interface to query them or perform high-level ETL operations.

## Key Features

* **Declarative & Programmatic:** Configure sources via a simple `config.yml` or build connections on-the-fly in Python using a fluent builder API.
* **Multi-Source Support:** Out-of-the-box support for **PostgreSQL**, **S3**, **SQLite**, and composite **DuckLake** sources (e.g., Postgres catalog + S3 storage).
* **Intelligent Plugin Management:** Automatically installs and loads the required DuckDB extensions based on your configuration.
* **Flexible Secret Management:** Pluggable secret provider system that defaults to environment variables but can be extended for services like AWS Secrets Manager or HashiCorp Vault.
* **High-Level ETL Utilities:** A powerful `move_data` function to transfer data between any two configured sources with a single line of code.

---

## Installation

Install the base library using pip:

```bash
pip install quackpipe
```

The library uses optional dependencies to keep the installation minimal. Install the support for the sources you need:

```bash
# To install support for postgres, s3, and ducklake
pip install "quackpipe[postgres,s3,ducklake]"
```

---

## Project Structure

The project is organized with a clear separation of concerns.

```
.
├── pyproject.toml              # Project configuration and dependencies
├── README.md                   # This file
├── src/
│   └── quackpipe/              # Main package
│       ├── __init__.py         # Public API exports
│       ├── core.py             # Core session management
│       ├── config.py           # Typed configuration objects (SourceConfig, SourceType)
│       ├── secrets.py          # Secret management system
│       ├── builder.py          # Programmatic builder API
│       ├── utils.py            # General utility functions (e.g., get_configs)
│       ├── etl_utils.py        # High-level ETL functions (e.g., move_data)
│       ├── exceptions.py       # Custom exceptions
│       └── sources/            # Source-specific logic
│           ├── base.py
│           ├── postgres.py
│           ├── s3.py
│           ├── sqlite.py
│           └── ducklake.py
├── examples/                   # Usage examples
│   ├── config.yml
│   └── run_etl.py
└── tests/                      # Pytest tests
    ├── test_etl_utils.py
    ├── test_integration.py
    └── ...
```

---

## Configuration (`config.yml`)

Define all your data sources in a `config.yml` file.

```yaml
# examples/config.yml
sources:
  # A writeable PostgreSQL database.
  pg_warehouse:
    type: postgres
    secret_name: "pg_prod" # See Secret Management section below
    read_only: false       # Allows writing data back to this source
    tables:                # Auto-create views for these tables
      - users
      - orders

  # An S3 data lake for Parquet files.
  s3_datalake:
    type: s3
    secret_name: "aws_prod"
    region: "us-east-1"

  # A local SQLite database file.
  local_analytics:
    type: sqlite
    path: "/path/to/analytics.db"
    read_only: true

  # A composite DuckLake source.
  my_lake:
    type: ducklake
    catalog:
      type: sqlite
      path: "/path/to/lake_catalog.db"
    storage:
      type: local
      path: "/path/to/lake_storage/"
```

### Secret Management

Quackpipe uses a `secret_name` to refer to a bundle of credentials. By default, it uses the `EnvSecretProvider`, which reads credentials from environment variables based on a convention: `SECRET_NAME_KEY`.

For a `secret_name` of **`pg_prod`**, you would set the following environment variables:

```bash
export PG_PROD_HOST=db.example.com
export PG_PROD_USER=myuser
export PG_PROD_PASSWORD=mypassword
export PG_PROD_DATABASE=production
```

---

## Usage Examples

Quackpipe offers multiple ways to interact with your data.

### 1. Interactive Session with YAML

The `session` context manager is perfect for exploration and running custom queries. It yields a pre-configured DuckDB connection.

```python
# examples/run_etl.py
import quackpipe

# This session will attach pg_warehouse and s3_datalake
with quackpipe.session(config_path="examples/config.yml", sources=["pg_warehouse", "s3_datalake"]) as con:
    # Query data from the attached postgres database
    active_users = con.execute("SELECT * FROM pg_warehouse_users WHERE status = 'active';").fetchdf()
    print(active_users.head())
```

### 2. High-Level Data Movement

The `move_data` utility is the easiest way to perform ETL. It's a self-contained function that handles the entire process of connecting, copying, and closing the connection.

```python
# examples/run_etl.py
from quackpipe.etl_utils import move_data

# Move active users from Postgres to a Parquet file in the S3 data lake
move_data(
    config_path="examples/config.yml",
    source_query="SELECT id, email, signup_date FROM pg_warehouse_users WHERE status = 'active'",
    destination_name="s3_datalake",
    table_name="active_users" # This will become active_users.parquet
)

# Move aggregated data from Postgres back into a new table in the same Postgres DB
move_data(
    config_path="examples/config.yml",
    source_query="SELECT signup_date, COUNT(*) as new_users FROM pg_warehouse_users GROUP BY 1",
    destination_name="pg_warehouse", # The destination is the writeable Postgres DB
    table_name="daily_signups",      # Creates a new table named 'daily_signups'
    mode="replace"                   # Options are 'replace' or 'append'
)
```

### 3. Programmatic Builder

For dynamic workflows where a YAML file is not suitable, use the `QuackpipeBuilder`.

```python
# examples/run_etl.py
from quackpipe import QuackpipeBuilder, SourceType

builder = (
    QuackpipeBuilder()
    .add_source(
        name="pg_main",
        type=SourceType.POSTGRES,
        secret_name="pg_prod",
        config={"read_only": True}
    )
    .add_source(
        name="local_db",
        type=SourceType.SQLITE,
        config={"path": "local.db"}
    )
)

# The builder returns a session context manager
with builder.session() as con:
    df = con.execute("SELECT * FROM pg_main.some_table;").fetchdf()
    print(df)
```

## Running Tests

To run the test suite, install the development dependencies and run `pytest`.

```bash
pip install ".[test]"
pytest

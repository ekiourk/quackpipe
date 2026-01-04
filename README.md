# Quackpipe

**The missing link between your Python scripts and your data infrastructure.**

Quackpipe is a powerful ETL helper library that uses **DuckDB** to create a unified, high-performance data plane for Python applications. It bridges the gap between writing raw, complex connection code and adopting a full-scale data transformation framework.

With a simple YAML configuration, you can instantly connect to multiple data sources like **PostgreSQL**, **S3**, **Azure Blob Storage**, and **SQLite**, and even orchestrate complex **DuckLake** setups, all from a single, clean Python interface.

[![codecov](https://codecov.io/github/ekiourk/quackpipe/graph/badge.svg?token=5LF2QD9MEW)](https://codecov.io/github/ekiourk/quackpipe)

## What Gap Does Quackpipe Fill?

In the modern data stack, you often face a choice:

* **Low-Level:** Write boilerplate code with multiple database drivers (`psycopg2`, `boto3`, etc.) to connect and move data manually. This is flexible but repetitive and error-prone.
* **High-Level:** Adopt a full DataOps framework like **SQLMesh** or **dbt**. These are powerful for building production-grade data warehouses but can be overkill for ad-hoc analysis, rapid prototyping, or simple scripting.

**Quackpipe provides the perfect middle ground.** It gives you the power of a unified query engine and the simplicity of a Python library, allowing you to:

* **Prototype Rapidly:** Spin up a multi-source data environment in seconds.
* **Simplify ETL Scripts:** Replace complex driver code with a single, clean `session` or a one-line `move_data` command.
* **Explore Data Interactively:** Use the built-in CLI to launch a web UI with all your sources pre-connected for instant ad-hoc querying.
* **Bridge to Production:** Automatically generate configuration for frameworks like **SQLMesh** when you're ready to graduate from a script to a versioned data model.

## Core Capabilities

* **Unified Data Access:** Query across PostgreSQL, S3, Azure, and SQLite as if they were all schemas in a single database.
* **Declarative Configuration:** Define all your data sources in one human-readable `config.yml` file.
* **Powerful ETL Utilities:** Move data between any two configured sources with the `move_data()` function.
* **Programmatic API:** Use the `QuackpipeBuilder` for dynamic, on-the-fly connection setups in your code.
* **Secure Secret Management:** Load credentials safely from `.env` files, keeping them out of your code and configuration.
* **Interactive UI:** Launch an interactive DuckDB web UI with all your sources pre-connected using a single CLI command.
* **Framework Integration:** Automatically generate a `sqlmesh_config.yml` file to seamlessly transition your project to a full DataOps framework.

## Installation

```bash
pip install quackpipe
```

Install support for the sources you need:

```bash
# Example: Install support for Postgres, S3, Azure, and the UI
pip install "quackpipe[postgres,s3,azure,ui]"
```

## Configuration

`quackpipe` uses a simple `config.yml` file to define your sources and an `.env` file to manage your secrets.

### Configuration Priority & Hierarchical Merging

Quackpipe supports loading configuration from multiple sources, merging them into a single final configuration. This allows you to have a base configuration and override specific parts for different environments (e.g., `base.yml` + `dev.yml`).

The configuration is loaded and merged in the following order (later sources override earlier ones):

1.  **Environment Variable (`QUACKPIPE_CONFIG_PATH`):** You can set this to a single path or multiple paths separated by your system's path separator (e.g., `:` on Linux/macOS, `;` on Windows).
2.  **CLI Arguments / Function Arguments:** When using the CLI, you can pass one or more file paths using `-c/--config`. When using the Python API, you can pass a list of paths to `config_path`.
3.  **Direct `configs` list:** In the Python API, passing a list of `SourceConfig` objects directly always takes the highest priority.

**Merging Logic:**
*   **Dictionaries (e.g., `sources`):** Deep merged. Keys in later configs overwrite keys in earlier ones. New keys are added.
*   **Lists (e.g., `before_all_statements`):** Replaced. If a later config defines a list, it completely replaces the list from earlier configs.
*   **Values:** Overwritten.

### `config.yml` Example

```yaml
# config.yml
sources:
  # A writeable PostgreSQL database.
  pg_warehouse:
    type: postgres
    secret_name: "pg_prod" # See Secret Management section below
    read_only: false       # Allows writing data back to this source

  # An S3 data lake for Parquet files.
  s3_datalake:
    type: s3
    secret_name: "aws_prod"
    region: "us-east-1"

  # An Azure Blob Storage container.
  azure_datalake:
    type: azure
    provider: connection_string
    secret_name: "azure_prod"

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

### Secret Management with `.env`

Quackpipe uses a `secret_name` in the config to refer to a bundle of credentials. These are loaded from `.env` files using a simple prefix convention: `SECRET_NAME_KEY`.

Create an `.env` file in your project root:

```dotenv
# .env

# Secrets for secret_name: "pg_prod"
PG_PROD_HOST=db.example.com
PG_PROD_USER=myuser
PG_PROD_PASSWORD=mypassword
PG_PROD_DATABASE=production

# Secrets for secret_name: "aws_prod"
AWS_PROD_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY
AWS_PROD_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_KEY

# Secrets for secret_name: "azure_prod"
AZURE_PROD_CONNECTION_STRING="DefaultEndpointsProtocol=https..."
```

**Multiple Environment Files:**
You can load secrets from multiple files. This is useful for separating default/shared configuration from local secrets.

```bash
# Load base.env first, then override with .env.local
quackpipe ui --env-file base.env .env.local
```

The "last one wins" rule applies: variables in later files will overwrite those in earlier files.

## Usage Highlights

### 1. Interactive Querying with `session`

Need to join a CSV in S3 with a table in Postgres? `quackpipe` makes it trivial.

```python
import quackpipe

# quackpipe automatically loads your .env file
with quackpipe.session(config_path="config.yml", env_file=".env") as con:
    df = con.execute("""
        SELECT u.name, o.order_total
        FROM pg_warehouse.users u
        JOIN read_parquet('s3://my-bucket/orders/*.parquet') o ON u.id = o.user_id
        WHERE u.signup_date > '2024-01-01';
    """).fetchdf()

    print(df.head())
```

### 2. One-Line Data Movement with `move_data`

Archive old records from your production database to your data lake with a single command.

```python
from quackpipe.etl_utils import move_data

move_data(
    config_path="config.yml",
    env_file=".env",
    source_query="SELECT * FROM pg_warehouse.logs WHERE timestamp < '2024-01-01'",
    destination_name="s3_datalake",
    table_name="logs_archive_2023"
)
```

### 3. Instant Data Exploration with the CLI

Launch a web browser UI with all your sources attached and ready for ad-hoc queries.

```bash
# This command reads your config.yml and .env file
quackpipe ui

# Or connect to specific sources
quackpipe ui pg_warehouse s3_datalake
```

### 4. Validate Your Configuration

Before running your scripts, you can validate your `config.yml` file (or a set of merged files) against the built-in schema to catch errors early.

```bash
# Validate the default config.yml
quackpipe validate

# Or validate multiple files (they will be merged)
quackpipe validate --config base.yml dev.yml
```

If the configuration is valid, you'll see a success message:
```
✅ Configuration from '['base.yml', 'dev.yml']' is valid.
```

If it's invalid, `quackpipe` will tell you why:
```
❌ Configuration is invalid.
   Reason: 'port' in source 'pg_main' should be an integer.
```

### 5. Inspect Merged Configuration

When using multiple configuration files, it can be helpful to see the final result after merging.

```bash
quackpipe preview-config -c base.yml dev.yml
```

This will print the full, merged YAML configuration to the console.

## Development

To set up the development environment for `quackpipe`, we recommend using [uv](https://github.com/astral-sh/uv).

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/ekiourk/quackpipe.git
    cd quackpipe
    ```

2.  **Install dependencies:**
    This command will create a virtual environment and install the package in editable mode along with all optional dependencies (including dev tools, fixtures, linting, etc.):
    ```bash
    uv sync --all-extras
    ```

3.  **Run tests:**
    Validate your setup by running the test suite with `pytest`:
    ```bash
    uv run pytest
    ```


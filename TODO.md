# Quackpipe TODO

A living document for planned improvements and new features, organised by theme.

---

## New Source Handlers

New handler modules to add, following the existing pattern in `src/quackpipe/sources/`.

### Iceberg (DuckDB 1.4)
Full read **and write** support for Apache Iceberg tables via REST catalogs.

- New `SourceType.ICEBERG` and `IcebergHandler` using `ATTACH … TYPE iceberg`
- Config fields: `endpoint`, `client_id`, `client_secret`, `warehouse`, `catalog_name`
- Enables Iceberg as a destination in `move_data()` (`COPY FROM DATABASE … TO iceberg_lake`)

```yaml
# Example quackpipe config
sources:
  my_iceberg:
    type: iceberg
    secret_name: iceberg_creds  # CLIENT_ID / CLIENT_SECRET
    endpoint: "http://rest-catalog:8181"
    warehouse: "s3://my-bucket/warehouse"
```

### Teradata (DuckDB 1.4)
Enterprise DWH connector analogous to the existing Postgres/MySQL handlers.

- New `SourceType.TERADATA` and `TeradataHandler` using `ATTACH … TYPE teradata`
- Config fields: `host`, `database`, `user`, `password`, `read_only`
- No new Python dependencies required — DuckDB ships the extension

### Delta Lake
Read (and potentially write) Delta Lake tables.

- New `SourceType.DELTA` and `DeltaHandler` using `delta_scan()` or `ATTACH`
- Config fields: `path` (local or cloud URI), `secret_name` for cloud auth

### Vortex (DuckDB core extension, Jan 2026)
Read and write [Vortex](https://vortex.dev/) files — a next-generation open columnar format donated to the Linux Foundation by SpiralDB and available as a **core** DuckDB extension. Vortex uses late-materialisation and compute-on-compressed-data techniques, making it well-suited for analytics, ML pre-processing pipelines and AI training workloads.

- New `SourceType.VORTEX` and `VortexHandler` (similar to the existing `ParquetHandler` and `CsvHandler`)
- Config fields: `path` (local or cloud URI, supports globs), `secret_name` for cloud auth
- Write support: the `move_data()` format `'vortex'` should emit `COPY … TO 'file.vortex' (FORMAT vortex)`
- Extension loaded via `INSTALL vortex; LOAD vortex;` — no Python dependencies

```yaml
# Example quackpipe config
sources:
  my_vortex_dataset:
    type: vortex
    path: "s3://my-bucket/data/*.vortex"
    secret_name: aws_prod
```

### Excel (DuckDB 1.2)
Read and write `.xlsx` files via the `excel` extension.

- New `SourceType.EXCEL` and `ExcelHandler`
- Config fields: `path`, `sheet` (optional)
- Useful for business-facing pipelines ingesting spreadsheets

---

## Refactor: Expression-Based Secrets

**Context:**
Currently every handler calls `fetch_secret_bundle()` in its `__init__`, which resolves the env-var values into Python memory and then interpolates them as **literal strings** directly into the SQL it generates.
For example, `PostgresHandler` ends up emitting:

```sql
CREATE OR REPLACE SECRET pg_prod_secret (
  TYPE POSTGRES,
  HOST 'db.prod.example.com',
  PASSWORD 'my-actual-password'   -- ← literal in the query string
);
```

This creates two problems:

1. Secret values appear verbatim in any SQL log, audit trail or debug output.
2. Quackpipe is responsible for secret resolution, coupling Python's side-channel env access to the DuckDB session lifetime.

**Existing Partial Implementation — SQLMesh Workaround:**
The `generate-sqlmesh-config` command (`commands/generate_sqlmesh_config.py`) already has a workaround for this exact problem, but takes a fragile approach: it first generates the SQL with fully-resolved literal secrets, then does a **post-hoc string replacement** via `_replace_secrets_with_placeholders()` to swap the literal values back out for `${ENV_VAR_NAME}` placeholders:

```python
# generate_sqlmesh_config.py — current approach
raw_sql = _generate_raw_sql(quackpipe_configs)               # SQL with plain-text secrets
final_sql = _replace_secrets_with_placeholders(raw_sql, ...)  # then scrubs them back out
```

The key building block already exists: **`fetch_raw_secret_bundle(secret_name)`** in `secrets.py` returns `{FULL_ENV_VAR_NAME: value}` (e.g. `{'PROD_DB_HOST': 'db.host.com'}`), which means the mapping from field → env-var name is already accessible. The SQLMesh command uses the value half to find-and-replace, but we should instead be using the key half to drive SQL generation upstream.

**Goal:**
Replace the post-hoc string replacement with upstream expression generation. Use DuckDB 1.3's expression support in `CREATE SECRET` to let **DuckDB itself** resolve secret values at runtime using `getenv()`, keeping plain-text credentials out of Python memory and generated SQL entirely.

The target output would be:

```sql
CREATE OR REPLACE SECRET pg_prod_secret (
  TYPE POSTGRES,
  HOST getenv('PROD_DB_HOST'),
  USER getenv('PROD_DB_USER'),
  PASSWORD getenv('PROD_DB_PASSWORD')
);
```

**What would change:**

- Add a `secrets.get_env_var_name(secret_name, field)` helper that returns the env-var name (e.g. `"PROD_DB_HOST"`) using `fetch_raw_secret_bundle()` — never the value.
- `fetch_secret_bundle()` is no longer called in handler `__init__` or `render_sql()` for secret injection.
- The `render_create_secret_sql()` method in each handler emits `getenv('ENV_VAR_NAME')` expressions instead of interpolated literals.
- The existing `validate()` / `resolve_secrets=True` path still uses `fetch_secret_bundle()` as a pre-flight check — that remains unchanged.
- The `_replace_secrets_with_placeholders()` workaround in `generate_sqlmesh_config.py` becomes **unnecessary** once handlers generate expressions upstream, and can be deleted, simplifying the SQLMesh command significantly.
- If a custom secret provider is needed (e.g. Vault, AWS Secrets Manager), a `raw_secret_expression` config field could let users inject arbitrary DuckDB expressions directly.

**Migration path:**
Fully backward-compatible — the env-var naming convention (`SECRET_NAME_FIELD`) is unchanged.


---

## `global_settings` YAML Config Block

Add a first-class `global_settings` section to the quackpipe YAML schema that maps directly to DuckDB `SET` statements, emitted before any source is attached.

```yaml
global_settings:
  memory_limit: "8GB"
  threads: 4
  enable_external_file_cache: true   # DuckDB 1.3: speeds up repeated S3/HTTP queries
  allowed_directories:               # DuckDB 1.2: filesystem sandboxing
    - "/app/data/"
    - "/tmp/"
```

- Add schema entries in `config.schema.yml`
- Parse in `get_global_statements()` and emit appropriate `SET` statements in `before_all_statements`
- List-type settings (`allowed_directories`) should be serialised as DuckDB list literals

---

## `ATTACH OR REPLACE` / Source Hot-Swap API (DuckDB 1.3)

Add a `reload_source()` or `swap_source()` utility to `etl_utils.py` that replaces a live-attached database with an updated version using `ATTACH OR REPLACE`, enabling zero-downtime data refreshes in long-running sessions.

```python
from quackpipe.etl_utils import swap_source

with session(config_path="config.yml") as con:
    # ... run queries against 'my_lake' backed by taxi_v1.duckdb
    swap_source(con, source_name="my_lake", new_path="taxi_v2.duckdb")
    # ... subsequent queries now read from taxi_v2.duckdb transparently
```

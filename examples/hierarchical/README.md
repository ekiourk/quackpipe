# Hierarchical Configuration Examples

This directory demonstrates how to use Quackpipe's hierarchical configuration feature to manage settings across different environments (e.g., Development vs. Production).

## The Files

*   **`base.yml`**: Contains common configuration shared by all environments (e.g., source types, schema definitions, global settings).
*   **`dev.yml`**: Overrides and extends the base configuration for development (e.g., dev secrets, local SQLite sources, debugging settings).
*   **`prod.yml`**: Overrides the base configuration for production (e.g., prod secrets, read-only enforcement, additional analytics sources).

## How to Run

You can mix and match these files using the `-c` / `--config` flag. The order matters: later files override earlier ones.

### 1. Previewing the Merged Configuration

Use the `preview-config` command to see the final result of merging.

**Development Config:**
```bash
quackpipe preview-config -c base.yml dev.yml
```
*Notice how `app_db` gets the `secret_name: pg_dev` and `read_only: false`, and `local_scratch` is added.*

**Production Config:**
```bash
quackpipe preview-config -c base.yml prod.yml
```
*Notice how `app_db` gets `secret_name: pg_prod`, and `archive_s3` is added.*

### 2. Validating the Configuration

You can validate the merged result to ensure it adheres to the schema.

```bash
quackpipe validate -c base.yml dev.yml
```

### 3. Using in Application

When launching the UI or generating config, simply pass the files in the same way.

```bash
# Launch UI with Dev config
quackpipe ui -c base.yml dev.yml
```
```bash
# Generate SQLMesh config for Prod
quackpipe generate-sqlmesh-config -c base.yml prod.yml -o sqlmesh_prod.yml
```

## Secret Management

This feature pairs well with hierarchical environment files.

```bash
# Load base config + dev config AND base env + dev env
quackpipe ui -c base.yml dev.yml --env-file .env .env.dev
```

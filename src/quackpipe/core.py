"""
The core logic of quackpipe.
"""
import logging
from functools import wraps

import duckdb

from quackpipe.config import Plugin, SourceConfig, SourceParams, SourceType, get_configs, get_global_statements
from quackpipe.exceptions import ConfigError
from quackpipe.secrets import configure_secret_provider, fetch_secret_bundle

# Import all handlers
from quackpipe.sources import azure_blob, ducklake, mysql, postgres, s3, sqlite

logger = logging.getLogger(__name__)

# The registry stores the handler CLASSES, not instances.
SOURCE_HANDLER_REGISTRY = {
    SourceType.POSTGRES: postgres.PostgresHandler,
    SourceType.MYSQL: mysql.MySQLHandler,
    SourceType.S3: s3.S3Handler,
    SourceType.AZURE: azure_blob.AzureBlobHandler,
    SourceType.DUCKLAKE: ducklake.DuckLakeHandler,
    SourceType.SQLITE: sqlite.SQLiteHandler,
}


def _prepare_connection(con: duckdb.DuckDBPyConnection, configs: list[SourceConfig]):
    """Configures a DuckDB connection from a list of SourceConfig objects."""
    if not configs:
        return

    # 1. Instantiate all handlers first
    instantiated_handlers = []
    for cfg in configs:
        HandlerClass = SOURCE_HANDLER_REGISTRY.get(cfg.type)
        if not HandlerClass:
            logger.warning("Warning: No handler class found for source type '%s'. Skipping.", cfg.type.value)
            continue

        full_context = {
            **cfg.config,
            "connection_name": cfg.name,
            "secret_name": cfg.secret_name,
            "before_source_statements": cfg.before_source_statements,
            "after_source_statements": cfg.after_source_statements,
        }
        handler_instance = HandlerClass(full_context)
        instantiated_handlers.append(handler_instance)

    # 2. Gather all required plugins from the instantiated handlers
    required_plugins = set()
    for handler in instantiated_handlers:
        required_plugins.update(handler.required_plugins)

    # 3. Install and load all extensions
    for plugin_def in required_plugins:
        if isinstance(plugin_def, Plugin):
            # It's a structured Plugin object with extra parameters
            plugin_name = plugin_def.name
            install_params = {'repository': plugin_def.repository}
            # Filter out None values to avoid passing `repository=None`
            clean_params = {k: v for k, v in install_params.items() if v is not None}
            con.install_extension(plugin_name, **clean_params)
        else:
            # It's a simple string (the name of the plugin)
            plugin_name = plugin_def
            con.install_extension(plugin_name)

        # Loading the extension only requires the name
        con.load_extension(plugin_name)

    # 4. Render and execute the setup SQL for each handler
    for handler in instantiated_handlers:
        # Execute any before_source_statements
        if handler.before_source_statements:
            for custom_sql in handler.before_source_statements:
                logger.debug("Executing custom SQL for %s:\n%s", handler.source_type, custom_sql)
                try:
                    con.execute(custom_sql)
                except (duckdb.ParserException, duckdb.IOException):
                    logger.exception("Error executing custom SQL for %s", handler.source_type)
                    raise

        # Execute the handler's main setup SQL
        setup_sql = handler.render_sql()
        if setup_sql:
            logger.debug("Executing setup SQL for %s:\n%s", handler.source_type, setup_sql)
            try:
                con.execute(setup_sql)
            except (duckdb.ParserException, duckdb.IOException):
                logger.exception("Error executing setup SQL for %s", handler.source_type)
                raise

        # Execute any additional custom SQL commands
        if handler.after_source_statements:
            for custom_sql in handler.after_source_statements:
                logger.debug("Executing custom SQL for %s:\n%s", handler.source_type, custom_sql)
                try:
                    con.execute(custom_sql)
                except (duckdb.ParserException, duckdb.IOException):
                    logger.exception("Error executing custom SQL for %s", handler.source_type)
                    raise


def session(
        config_path: str | list[str] | None = None,
        configs: list[SourceConfig] | None = None,
        sources: list[str] | None = None,
        env_file: str | list[str] | None = None,
) -> duckdb.DuckDBPyConnection:
    """
    Creates and returns a pre-configured DuckDB connection.

    The returned connection object is a context manager and can be used in a
    `with` statement, which will automatically handle closing the connection.

    Configuration can be provided via the `config_path` parameter, the
    `QUACKPIPE_CONFIG_PATH` environment variable, or by passing a list of
    `SourceConfig` objects to the `configs` parameter.

    Example:
        # As a context manager
        with session(config_path="config.yml") as con:
            con.sql("SELECT * FROM my_table")

        # As a direct function call
        con = session(config_path="config.yml")
        # Remember to close it yourself
        con.close()
    """
    configure_secret_provider(env_file=env_file)

    all_configs = get_configs(config_path, configs)

    active_configs = all_configs
    if sources:
        active_configs = [c for c in all_configs if c.name in sources]

    con = duckdb.connect(database=':memory:')

    global_statements = get_global_statements(config_path)

    # Execute before_all_statements
    for stmt in global_statements.get('before_all_statements', []):
        con.execute(stmt)

    _prepare_connection(con, active_configs)

    # Execute after_all_statements
    for stmt in global_statements.get('after_all_statements', []):
        con.execute(stmt)

    return con


def with_session(**session_kwargs):
    """
    A decorator to inject a pre-configured DuckDB connection into a function.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with session(**session_kwargs) as con:
                return func(con, *args, **kwargs)

        return wrapper

    return decorator


def get_source_params(
    source_name: str,
    config_path: str | list[str] | None = None,
    env_file: str | list[str] | None = None,
) -> SourceParams:
    """
    Returns the configuration for a given source, merged with its secrets.

    Args:
        source_name: The name of the source to get the configuration for.
        config_path: The path to the configuration file.
        env_file: The path to the environment file.

    Returns:
        A dictionary containing the merged configuration and secrets.
    """
    configure_secret_provider(env_file=env_file)

    all_configs = get_configs(config_path)
    source_config = next((c for c in all_configs if c.name == source_name), None)

    if not source_config:
        raise ConfigError(f"Source '{source_name}' not found in configuration.")

    secrets = fetch_secret_bundle(source_config.secret_name)
    return SourceParams({**source_config.config, **secrets})

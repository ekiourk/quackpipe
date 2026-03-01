"""
The core logic of quackpipe.
"""

import logging
from collections.abc import Callable
from functools import wraps
from typing import Any

import duckdb

from quackpipe.config import Plugin, SourceConfig, SourceParams, get_configs, get_global_statements
from quackpipe.exceptions import (
    ExecutionError,
    ExtensionError,
    ValidationError,
)
from quackpipe.secrets import configure_secret_provider, fetch_secret_bundle

# Import the registry of handlers
from quackpipe.sources import SOURCE_HANDLER_REGISTRY

__all__ = ["session", "with_session", "get_source_params", "SOURCE_HANDLER_REGISTRY"]

logger = logging.getLogger(__name__)


def _prepare_connection(con: duckdb.DuckDBPyConnection, configs: list[SourceConfig]) -> None:
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
        plugin_name = plugin_def.name if isinstance(plugin_def, Plugin) else plugin_def
        try:
            if isinstance(plugin_def, Plugin):
                # It's a structured Plugin object with extra parameters
                install_params = {"repository": plugin_def.repository}
                # Filter out None values to avoid passing `repository=None`
                clean_params = {k: v for k, v in install_params.items() if v is not None}
                con.install_extension(plugin_name, **clean_params)
            else:
                # It's a simple string (the name of the plugin)
                con.install_extension(plugin_name)

            # Loading the extension only requires the name
            con.load_extension(plugin_name)
        except (duckdb.IOException, duckdb.HTTPException) as e:
            raise ExtensionError(f"Failed to install or load extension '{plugin_name}': {e}") from e

    # 4. Render and execute the setup SQL for each handler
    for handler in instantiated_handlers:
        # Execute any before_source_statements
        if handler.before_source_statements:
            for custom_sql in handler.before_source_statements:
                logger.debug("Executing custom SQL for %s:\n%s", handler.source_type, custom_sql)
                try:
                    con.execute(custom_sql)
                except (duckdb.ParserException, duckdb.IOException) as e:
                    raise ExecutionError(f"Error executing custom 'before' SQL for {handler.source_type}: {e}") from e

        # Execute the handler's main setup SQL
        try:
            setup_sql = handler.render_sql()
        except Exception as e:
            raise ExecutionError(f"Error rendering setup SQL for {handler.source_type}: {e}") from e

        if setup_sql:
            logger.debug("Executing setup SQL for %s:\n%s", handler.source_type, setup_sql)
            try:
                con.execute(setup_sql)
            except (duckdb.ParserException, duckdb.IOException, duckdb.HTTPException) as e:
                raise ExecutionError(f"Error executing setup SQL for {handler.source_type}: {e}") from e

        # Execute any additional custom SQL commands
        if handler.after_source_statements:
            for custom_sql in handler.after_source_statements:
                logger.debug("Executing custom SQL for %s:\n%s", handler.source_type, custom_sql)
                try:
                    con.execute(custom_sql)
                except (duckdb.ParserException, duckdb.IOException) as e:
                    raise ExecutionError(f"Error executing custom 'after' SQL for {handler.source_type}: {e}") from e


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
        # Validate that all requested sources actually exist in the config
        all_names = {c.name for c in all_configs}
        missing = [s for s in sources if s not in all_names]
        if missing:
            raise ValidationError(
                f"The following requested sources were not found in the configuration: {', '.join(missing)}"
            )

        active_configs = [c for c in all_configs if c.name in sources]

    # Perform pre-flight validation (checking that both config and environment variables are present)
    for cfg in active_configs:
        HandlerClass = SOURCE_HANDLER_REGISTRY.get(cfg.type)
        if HandlerClass:
            HandlerClass.validate(cfg.config, cfg.secret_name, resolve_secrets=True)

    con = duckdb.connect(database=":memory:")

    global_statements = get_global_statements(config_path) if config_path else {}

    # Execute before_all_statements
    for stmt in global_statements.get("before_all_statements", []):
        con.execute(stmt)

    _prepare_connection(con, active_configs)

    # Execute after_all_statements
    for stmt in global_statements.get("after_all_statements", []):
        con.execute(stmt)

    return con


def with_session(**session_kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    A decorator to inject a pre-configured DuckDB connection into a function.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
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
        raise ValidationError(f"Source '{source_name}' not found in configuration.")

    secrets = fetch_secret_bundle(source_config.secret_name)
    return SourceParams({**source_config.config, **secrets})

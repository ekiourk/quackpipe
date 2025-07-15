"""
The core logic of quackpipe.
"""
import logging
from collections.abc import Generator
from contextlib import contextmanager
from functools import wraps

import duckdb

from quackpipe.config import Plugin, SourceConfig, SourceType
from quackpipe.secrets import configure_secret_provider

# Import all handlers
from quackpipe.sources import azure_blob, ducklake, postgres, s3, sqlite
from quackpipe.utils import get_configs

logger = logging.getLogger(__name__)

# The registry stores the handler CLASSES, not instances.
SOURCE_HANDLER_REGISTRY = {
    SourceType.POSTGRES: postgres.PostgresHandler,
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
        setup_sql = handler.render_sql()
        logger.debug(setup_sql)
        try:
            con.execute(setup_sql)
        except (duckdb.ParserException, duckdb.IOException):
            logger.exception(setup_sql)
            raise


@contextmanager
def session(
        config_path: str | None = None,
        configs: list[SourceConfig] | None = None,
        sources: list[str] | None = None,
        env_file: str | None = None
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    A context manager providing a pre-configured DuckDB connection.
    """
    configure_secret_provider(env_file=env_file)

    all_configs = get_configs(config_path, configs)

    active_configs = all_configs
    if sources:
        active_configs = [c for c in all_configs if c.name in sources]

    con = duckdb.connect(database=':memory:')
    try:
        _prepare_connection(con, active_configs)
        yield con
    finally:
        con.close()


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

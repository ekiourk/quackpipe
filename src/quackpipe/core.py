"""
The core logic of quackpipe.
"""
from contextlib import contextmanager
from functools import wraps
from typing import List, Optional, Generator

import duckdb

from .config import SourceConfig, SourceType
# Import all handlers
from .sources import s3, postgres, ducklake, sqlite
from .utils import get_configs

# The registry now stores the handler CLASSES, not instances.
SOURCE_HANDLER_REGISTRY = {
    SourceType.POSTGRES: postgres.PostgresHandler,
    SourceType.S3: s3.S3Handler,
    SourceType.DUCKLAKE: ducklake.DuckLakeHandler,
    SourceType.SQLITE: sqlite.SQLiteHandler,
}


def _prepare_connection(con: duckdb.DuckDBPyConnection, configs: List[SourceConfig]):
    """Configures a DuckDB connection from a list of SourceConfig objects."""
    if not configs:
        return

    # 1. Instantiate all handlers first
    instantiated_handlers = []
    for cfg in configs:
        HandlerClass = SOURCE_HANDLER_REGISTRY.get(cfg.type)
        if not HandlerClass:
            print(f"Warning: No handler class found for source type '{cfg.type.value}'. Skipping.")
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
    for plugin in required_plugins:
        con.install_extension(plugin)
        con.load_extension(plugin)

    # 4. Render and execute the setup SQL for each handler
    for handler in instantiated_handlers:
        setup_sql = handler.render_sql()
        con.execute(setup_sql)


@contextmanager
def session(
        config_path: Optional[str] = None,
        configs: Optional[List[SourceConfig]] = None,
        sources: Optional[List[str]] = None
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    A context manager providing a pre-configured DuckDB connection.
    """
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

"""
The core logic of quackpipe.
"""
from contextlib import contextmanager
from functools import wraps
from typing import List, Optional, Generator

import duckdb
import yaml

from .config import SourceConfig, SourceType
from .exceptions import ConfigError
from .secrets import fetch_secret_bundle
from .sources import s3, postgres, ducklake

# Registry mapping string types to handler classes
SOURCE_HANDLER_REGISTRY = {
    SourceType.POSTGRES: postgres.PostgresHandler(),
    SourceType.S3: s3.S3Handler(),
    SourceType.DUCKLAKE: ducklake.DuckLakeHandler(),
}

def _parse_config_from_yaml(path: str) -> List[SourceConfig]:
    """Loads a YAML file and parses it into a list of SourceConfig objects."""
    try:
        with open(path, 'r') as f:
            raw_config = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigError(f"Configuration file not found at '{path}'.")

    source_configs = []
    for name, details in raw_config.get('sources', {}).items():
        details_copy = details.copy()

        # Pop the known, top-level fields for the SourceConfig object itself.
        try:
            source_type_str = details_copy.pop('type')
            source_type = SourceType(source_type_str)
        except (KeyError, ValueError):
            raise ConfigError(f"Missing or invalid 'type' for source '{name}'.")

        secret_name = details_copy.pop('secret_name', None)

        # Everything that remains in the dictionary is source-specific config.
        source_specific_config = details_copy

        # Now, construct the SourceConfig object correctly.
        source_configs.append(SourceConfig(
            name=name,
            type=source_type,
            secret_name=secret_name,
            config=source_specific_config  # Pass the rest as the 'config' dict
        ))
    return source_configs


def _prepare_connection(con: duckdb.DuckDBPyConnection, configs: List[SourceConfig]):
    """Configures a DuckDB connection from a list of SourceConfig objects."""
    if not configs:
        return

    required_plugins = set()
    for cfg in configs:
        handler = SOURCE_HANDLER_REGISTRY.get(cfg.type)
        if handler:
            required_plugins.update(handler.required_plugins)

    for plugin in required_plugins:
        con.install_extension(plugin)
        con.load_extension(plugin)
    
    for cfg in configs:
        handler = SOURCE_HANDLER_REGISTRY.get(cfg.type)
        if not handler:
            continue

        secrets = fetch_secret_bundle(cfg.secret_name)
        full_context = {
            **cfg.config,
            **secrets,
            "connection_name": cfg.name,
        }
        
        setup_sql = handler.render_sql(full_context)
        con.execute(setup_sql)

@contextmanager
def session(
    config_path: Optional[str] = None,
    configs: Optional[List[SourceConfig]] = None,
    sources: Optional[List[str]] = None
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    A context manager providing a pre-configured DuckDB connection.
    Accepts configuration from a YAML file OR a list of SourceConfig objects.
    """
    if config_path:
        all_configs = _parse_config_from_yaml(config_path)
    elif configs:
        all_configs = configs
    else:
        raise ConfigError("Must provide either 'config_path' or 'configs'.")

    if sources:
        all_configs = [c for c in all_configs if c.name in sources]

    con = duckdb.connect(database=':memory:')
    try:
        _prepare_connection(con, all_configs)
        yield con
    finally:
        con.close()

def with_session(**session_kwargs):
    """A decorator to inject a pre-configured DuckDB connection."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with session(**session_kwargs) as con:
                return func(con, *args, **kwargs)
        return wrapper
    return decorator

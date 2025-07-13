"""
General utility functions for the quackpipe library.
"""

import yaml

# Note: We need to import these here to avoid circular dependencies
# if this module were to be used by the config module in the future.
from .config import SourceConfig
from .exceptions import ConfigError


def parse_config_from_yaml(path: str) -> list[SourceConfig]:
    """Loads a YAML file and parses it into a list of SourceConfig objects."""
    try:
        with open(path) as f:
            raw_config = yaml.safe_load(f)
    except FileNotFoundError as e:
        raise ConfigError(f"Configuration file not found at '{path}'.") from e

    source_configs = []
    for name, details in raw_config.get('sources', {}).items():
        details_copy = details.copy()

        try:
            # We import here to avoid a circular import at the top level
            from .config import SourceType
            source_type_str = details_copy.pop('type')
            source_type = SourceType(source_type_str)
        except (KeyError, ValueError) as e:
            raise ConfigError(f"Missing or invalid 'type' for source '{name}'.") from e

        secret_name = details_copy.pop('secret_name', None)
        source_specific_config = details_copy

        source_configs.append(SourceConfig(
            name=name,
            type=source_type,
            secret_name=secret_name,
            config=source_specific_config
        ))
    return source_configs


def get_configs(
        config_path: str | None = None,
        configs: list[SourceConfig] | None = None
) -> list[SourceConfig]:
    """
    A helper function to load source configurations from either a file path or a direct list.
    This logic is shared by `session` and `etl_utils`.
    """
    if config_path:
        return parse_config_from_yaml(config_path)
    elif configs:
        return configs
    else:
        # This provides a clear error message if no configuration source is given.
        raise ConfigError("Must provide either a 'config_path' or a 'configs' list.")

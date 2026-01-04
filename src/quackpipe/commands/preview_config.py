"""
src/quackpipe/commands/preview_config.py

This module contains the implementation for the 'preview-config' CLI command.
"""
from argparse import _SubParsersAction

import yaml

from ..config import get_config_yaml
from ..exceptions import ConfigError
from .common import get_default_config_path, normalize_arg_to_list


def handler(args):
    """The main handler function for the preview-config command."""
    config_paths = normalize_arg_to_list(args.config)
    try:
        merged_config = get_config_yaml(config_paths)
        if merged_config is None:
             raise ConfigError("No config file found. Please specify one with -c/--config or set QUACKPIPE_CONFIG_PATH.")

        print(yaml.dump(merged_config, sort_keys=False))

    except ConfigError as e:
        print(f"❌ Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def register_command(subparsers: _SubParsersAction):
    """Registers the command and its arguments to the main CLI parser."""
    parser = subparsers.add_parser(
        "preview-config",
        help="Preview the final merged configuration from multiple files."
    )
    parser.add_argument("-c", "--config", default=get_default_config_path(), nargs='+',
                                 help="Path(s) to the quackpipe config.yml file(s).")
    parser.set_defaults(func=handler)

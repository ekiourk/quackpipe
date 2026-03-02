"""
src/quackpipe/commands/preview_config.py

This module contains the implementation for the 'preview-config' CLI command.
"""

import argparse
from argparse import _SubParsersAction
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    SubParsersAction = _SubParsersAction[argparse.ArgumentParser]
else:
    SubParsersAction = _SubParsersAction


import yaml

from ..config import get_config_yaml
from ..exceptions import ConfigError
from .common import get_default_config_path, normalize_arg_to_list, setup_cli_logging


def handler(args: argparse.Namespace) -> None:
    """The main handler function for the preview-config command."""
    import sys

    log = setup_cli_logging(args.verbose)
    config_paths = normalize_arg_to_list(args.config)
    try:
        merged_config = get_config_yaml(config_paths)
        if merged_config is None:
            raise ConfigError("No config file found. Please specify one with -c/--config or set QUACKPIPE_CONFIG_PATH.")

        yaml_output = yaml.dump(merged_config, sort_keys=False)
        log.info("Merged configuration:")
        print(yaml_output)  # noqa: T201

    except ConfigError as e:
        log.error(f"❌ Error: {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


def register_command(subparsers: SubParsersAction) -> None:
    """Registers the command and its arguments to the main CLI parser."""
    parser = subparsers.add_parser("preview-config", help="Preview the final merged configuration from multiple files.")
    parser.add_argument(
        "-c",
        "--config",
        default=get_default_config_path(),
        nargs="+",
        help="Path(s) to the quackpipe config.yml file(s).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase output verbosity. Use -v for INFO and -vv for DEBUG.",
    )
    parser.set_defaults(func=handler)

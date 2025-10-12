"""
src/quackpipe/commands/validate.py

This module contains the implementation for the 'validate' CLI command.
"""
from argparse import _SubParsersAction

import yaml
from jsonschema.exceptions import ValidationError

from ..config import validate_config
from ..exceptions import ConfigError
from .common import get_default_config_path


def handler(args):
    """The main handler function for the validate command."""
    config_path = args.config
    print(f"Attempting to validate configuration file: {config_path}")

    try:
        if not config_path:
            raise ConfigError("No config file found. Please specify one with -c/--config or set QUACKPIPE_CONFIG_PATH.")

        with open(config_path) as f:
            raw_config = yaml.safe_load(f)

        validate_config(raw_config)
        print(f"✅ Configuration file at '{config_path}' is valid.")

    except FileNotFoundError:
        print(f"❌ Error: Configuration file not found at '{config_path}'.")
    except (ValidationError, ConfigError) as e:
        print(f"❌ Configuration file at '{config_path}' is invalid.")
        print(f"   Reason: {e.message}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def register_command(subparsers: _SubParsersAction):
    """Registers the command and its arguments to the main CLI parser."""
    parser_validate = subparsers.add_parser(
        "validate",
        help="Validate a quackpipe configuration file against the schema."
    )
    parser_validate.add_argument("-c", "--config", default=get_default_config_path(),
                                 help="Path to the quackpipe config.yml file. Defaults to 'config.yml' in the current "
                                      "directory if it exists or else it will check the "
                                      "QUACKPIPE_CONFIG_PATH environment variable.")
    parser_validate.set_defaults(func=handler)

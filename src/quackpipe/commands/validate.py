"""
src/quackpipe/commands/validate.py

This module contains the implementation for the 'validate' CLI command.
"""
from argparse import _SubParsersAction

from jsonschema.exceptions import ValidationError

from ..config import get_config_yaml, validate_config
from ..exceptions import ConfigError
from .common import get_default_config_path, normalize_arg_to_list, setup_cli_logging


def handler(args):
    """The main handler function for the validate command."""
    log = setup_cli_logging(args.verbose)
    config_paths = normalize_arg_to_list(args.config)
    log.info(f"Attempting to validate configuration from: {config_paths}")

    try:
        merged_config = get_config_yaml(config_paths)

        if merged_config is None:
            raise ConfigError("No config file found. Please specify one with -c/--config or set QUACKPIPE_CONFIG_PATH.")

        validate_config(merged_config)
        print(f"✅ Configuration from '{config_paths}' is valid.")

    except ValidationError as e:
        print("❌ Configuration is invalid.")
        print(f"   Reason: {e.message}")
    except ConfigError as e:
        print("❌ Configuration is invalid.")
        print(f"   Reason: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def register_command(subparsers: _SubParsersAction):
    """Registers the command and its arguments to the main CLI parser."""
    parser_validate = subparsers.add_parser(
        "validate",
        help="Validate a quackpipe configuration file (or merged files) against the schema."
    )
    parser_validate.add_argument("-c", "--config", default=get_default_config_path(), nargs='+',
                                 help="Path(s) to the quackpipe config.yml file(s). Defaults to 'config.yml' in the "
                                      "current directory if it exists or else it will check the "
                                      "QUACKPIPE_CONFIG_PATH environment variable.")
    parser_validate.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase output verbosity. Use -v for INFO and -vv for DEBUG."
    )
    parser_validate.set_defaults(func=handler)

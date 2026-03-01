"""
src/quackpipe/commands/validate.py

This module contains the implementation for the 'validate' CLI command.
"""

import argparse
from argparse import _SubParsersAction

from jsonschema.exceptions import ValidationError

from ..config import get_config_yaml, parse_config_from_yaml, validate_config
from ..exceptions import ConfigError
from ..secrets import configure_secret_provider
from .common import get_default_config_path, normalize_arg_to_list, setup_cli_logging


def handler(args: argparse.Namespace) -> None:
    """The main handler function for the validate command."""
    log = setup_cli_logging(args.verbose)
    config_paths = normalize_arg_to_list(args.config)
    env_file = normalize_arg_to_list(args.env_file) if hasattr(args, "env_file") else None

    log.info(f"Attempting to validate configuration from: {config_paths}")

    try:
        merged_config = get_config_yaml(config_paths)

        if merged_config is None:
            raise ConfigError("No config file found. Please specify one with -c/--config or set QUACKPIPE_CONFIG_PATH.")

        # Layer 1: Structural Schema Validation
        validate_config(merged_config)

        # Layer 2: Semantic Validation (and optional Secret Resolution)
        if args.resolve_secrets:
            log.info("Performing semantic validation with secret resolution...")
            configure_secret_provider(env_file=env_file)
            # parse_config_from_yaml internally calls validate() for each source
            # We need to pass the resolve_secrets flag down.
            # Since parse_config_from_yaml currently doesn't take it, we'll manually validate here
            # to avoid changing parse_config_from_yaml signature too much if possible,
            # OR we update parse_config_from_yaml to accept it.
            # Updating parse_config_from_yaml is cleaner.
            parse_config_from_yaml(merged_config, resolve_secrets=True)
        else:
            # Just do the basic semantic validation (without secrets)
            parse_config_from_yaml(merged_config, resolve_secrets=False)

        print(f"✅ Configuration from '{config_paths}' is valid.")

    except ValidationError as e:
        print("❌ Configuration is invalid.")
        print(f"   Reason: {e.message}")
    except ConfigError as e:
        print("❌ Configuration is invalid.")
        print(f"   Reason: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def register_command(subparsers: _SubParsersAction) -> None:
    """Registers the command and its arguments to the main CLI parser."""
    parser_validate = subparsers.add_parser(
        "validate", help="Validate a quackpipe configuration file (or merged files) against the schema."
    )
    parser_validate.add_argument(
        "-c",
        "--config",
        default=get_default_config_path(),
        nargs="+",
        help="Path(s) to the quackpipe config.yml file(s). Defaults to 'config.yml' in the "
        "current directory if it exists or else it will check the "
        "QUACKPIPE_CONFIG_PATH environment variable.",
    )
    parser_validate.add_argument(
        "-e", "--env-file", nargs="+", help="Path(s) to .env file(s) to load for secret resolution."
    )
    parser_validate.add_argument(
        "--resolve-secrets",
        action="store_true",
        help="If set, also validates that all required secrets are present in the environment.",
    )
    parser_validate.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase output verbosity. Use -v for INFO and -vv for DEBUG.",
    )
    parser_validate.set_defaults(func=handler)

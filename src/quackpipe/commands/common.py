"""
src/quackpipe/commands/common.py

This module contains common utilities shared across CLI command modules.
"""
import logging
import os
import sys

DEFAULT_CONFIG_NAME = "config.yml"


def get_default_config_path():
    """
    Returns 'config.yml' if it exists in the current directory, otherwise None.
    This allows for a dynamic default value in the CLI.
    """
    if os.path.exists(DEFAULT_CONFIG_NAME):
        return DEFAULT_CONFIG_NAME
    return None


def setup_cli_logging(verbose_level: int = 0):
    """
    Configures the root logger for quackpipe to ensure CLI output is visible.

    Args:
        verbose_level (int): The verbosity level. 0 for WARNING, 1 for INFO, 2+ for DEBUG.
    """
    # Map the integer verbosity level to a logging level
    if verbose_level >= 2:
        level = logging.DEBUG
    elif verbose_level == 1:
        level = logging.INFO
    else:
        # Default to WARNING to avoid being too noisy
        level = logging.WARNING

    # Get the top-level logger for the library
    log = logging.getLogger("quackpipe")
    log.setLevel(level)

    # Create a handler to write messages to the console (stdout)
    handler = logging.StreamHandler(sys.stdout)

    # Create a formatter and add it to the handler
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)

    # Add the handler to the logger. This ensures messages will be output.
    # We clear existing handlers to avoid duplicate messages if run in a notebook.
    if log.hasHandlers():
        log.handlers.clear()
    log.addHandler(handler)

    return log


def normalize_arg_to_list(arg: str | list[str] | None) -> list[str]:
    """
    Helper to normalize CLI arguments that might be a string (default), a list (nargs='+'), or None.
    Always returns a list of strings (empty if None).
    """
    if arg is None:
        return []
    if isinstance(arg, str):
        return [arg]
    return arg

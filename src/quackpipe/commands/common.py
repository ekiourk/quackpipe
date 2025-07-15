"""
src/quackpipe/commands/common.py

This module contains common utilities shared across CLI command modules.
"""
import logging
import sys


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

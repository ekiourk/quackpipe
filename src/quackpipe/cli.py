"""
cli.py

This module provides the main entry point for the quackpipe command-line interface.
It discovers and registers commands from the 'commands' submodule.
"""

import argparse

# Import the registration functions from each command module
from . import __version__
from .commands import generate_sqlmesh_config, preview_config, ui, validate


def main() -> None:
    """Main function to parse arguments and dispatch commands."""
    parser = argparse.ArgumentParser(description="quackpipe: A DuckDB ETL Helper CLI.")

    parser.add_argument(
        "-V", "--version", action="version", version=f"%(prog)s {__version__}", help="Show the version and exit."
    )

    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # Register all available commands
    generate_sqlmesh_config.register_command(subparsers)
    ui.register_command(subparsers)
    validate.register_command(subparsers)
    preview_config.register_command(subparsers)

    # Parse the arguments and call the handler function assigned by the subparser
    args = parser.parse_args()
    try:
        args.func(args)
    except Exception as e:
        import sys

        print(f"An unexpected error occurred: {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)


if __name__ == "__main__":
    main()

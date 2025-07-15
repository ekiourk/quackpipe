"""
cli.py

This module provides the main entry point for the quackpipe command-line interface.
It discovers and registers commands from the 'commands' submodule.
"""
import argparse

# Import the registration functions from each command module
from .commands import generate_sqlmesh_config, ui


def main():
    """Main function to parse arguments and dispatch commands."""
    parser = argparse.ArgumentParser(description="quackpipe: A DuckDB ETL Helper CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # Register all available commands
    generate_sqlmesh_config.register_command(subparsers)
    ui.register_command(subparsers)

    # Parse the arguments and call the handler function assigned by the subparser
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

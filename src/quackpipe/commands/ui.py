"""
src/quackpipe/commands/ui.py

This module contains the implementation for the 'ui' CLI command.
"""
from argparse import _SubParsersAction

from .. import ConfigError
from ..core import session
from .common import get_default_config_path, normalize_arg_to_list, setup_cli_logging


def handler(args):
    """The main handler function for the ui command."""
    log = setup_cli_logging(args.verbose)

    sources_to_load = args.sources if args.sources else "all configured sources"
    log.info(f"Attempting to start UI session for: {sources_to_load}")

    config_paths = normalize_arg_to_list(args.config)
    env_files = normalize_arg_to_list(args.env_file)
    log.debug(f"Using config file(s): {config_paths} and env file(s): {env_files}")

    try:
        with session(config_path=config_paths, env_file=env_files, sources=args.sources) as con:
            log.info("Session created.")

            log.info(f"Setting UI port to {args.port}...")
            con.execute(f"SET ui_local_port = {args.port};")

            log.info("Starting DuckDB UI server...")
            con.execute("CALL start_ui_server();")

            log.warning(f"✅ DuckDB UI is running at: http://localhost:{args.port}")
            log.info("All sources from your config are attached and ready to query.")

            try:
                # Wait for user input to keep the server alive.
                input("Press Enter or Ctrl+C to exit and shut down the UI server...")
            except KeyboardInterrupt:
                # Handle Ctrl+C gracefully by just printing a newline and proceeding.
                print()  # Move to the next line after the ^C character
                pass

            log.info("Stopping DuckDB UI server...")
            con.execute("CALL stop_ui_server();")

    except Exception as e:
        log_msg = f"❌ Failed to start UI session: {e}"
        if isinstance(e, ConfigError):
            log.warning(log_msg)
        else:
            log.error(log_msg, exc_info=True)
    finally:
        log.info("Shutting down.")


def register_command(subparsers: _SubParsersAction):
    """Registers the command and its arguments to the main CLI parser."""
    parser_ui = subparsers.add_parser(
        "ui",
        help="Launch an interactive DuckDB UI with pre-configured sources."
    )
    parser_ui.add_argument("-c", "--config", default=get_default_config_path(), nargs='+',
                            help="Path(s) to the quackpipe config.yml file(s). Defaults to 'config.yml' in the current "
                                 "directory if it exists or else it will check the "
                                 "QUACKPIPE_CONFIG_PATH environment variable.")
    parser_ui.add_argument("--env-file", default=[".env"], nargs='+',
                           help="Path(s) to the environment file(s) to load secrets from. (Default: .env)")
    parser_ui.add_argument("-p", "--port", type=int, default=4213,
                           help="Port to run the DuckDB UI on. (Default: 4213)")
    parser_ui.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase output verbosity. Use -v for INFO and -vv for DEBUG."
    )
    parser_ui.add_argument("sources", nargs='*',
                           help="Optional: A space-separated list of specific sources to load. "
                                "If omitted, all sources are loaded.")
    parser_ui.set_defaults(func=handler)

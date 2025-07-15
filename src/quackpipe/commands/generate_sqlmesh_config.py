"""
src/quackpipe/commands/generate_sqlmesh_config.py

This module contains the implementation for the 'generate-sqlmesh-config' CLI command.
"""
from argparse import _SubParsersAction

import yaml

from ..config import SourceConfig
from ..core import SOURCE_HANDLER_REGISTRY
from ..secrets import configure_secret_provider, fetch_raw_secret_bundle
from ..utils import get_configs


def _generate_raw_sql(configs: list[SourceConfig]) -> str:
    """Instantiates handlers and generates the full setup SQL with resolved secrets."""
    all_sql_statements = []
    for cfg in configs:
        HandlerClass = SOURCE_HANDLER_REGISTRY.get(cfg.type)
        if not HandlerClass:
            continue
        full_context = {**cfg.config, "connection_name": cfg.name, "secret_name": cfg.secret_name}
        handler_instance = HandlerClass(full_context)
        sql = handler_instance.render_sql()
        all_sql_statements.append(sql)
    return "\n\n".join(filter(None, all_sql_statements))


def _replace_secrets_with_placeholders(sql_string: str, configs: list[SourceConfig]) -> str:
    """Takes a raw SQL string and replaces secret values with environment variable placeholders."""
    final_sql = sql_string
    for cfg in configs:
        if cfg.secret_name:
            resolved_secrets = fetch_raw_secret_bundle(cfg.secret_name)
            value_to_placeholder = {}
            for env_var_name, value in resolved_secrets.items():
                placeholder = f"${{{env_var_name}}}"
                value_to_placeholder[f"'{value}'"] = f"'{placeholder}'"
                value_to_placeholder[str(value)] = placeholder
            for val, placeholder in sorted(value_to_placeholder.items(), key=lambda item: len(item[0]), reverse=True):
                final_sql = final_sql.replace(val, placeholder)
    return final_sql


def _build_sqlmesh_dict(init_sql_block: str, gateway_name: str, state_db: str) -> dict:
    """Constructs the Python dictionary for the SQLMesh config YAML."""
    return {'gateways': {gateway_name: {'connection': {'type': 'duckdb', 'init': init_sql_block},
                                        'state_connection': {'type': 'duckdb', 'database': state_db}}},
            'default_gateway': gateway_name}


def handler(args):
    """The main handler function for the generate-sqlmesh-config command."""
    configure_secret_provider(env_file=args.env_file)
    print(f"Reading quackpipe configuration from: {args.config}")
    quackpipe_configs = get_configs(config_path=args.config)
    raw_sql = _generate_raw_sql(quackpipe_configs)
    final_sql_with_placeholders = _replace_secrets_with_placeholders(raw_sql, quackpipe_configs)
    sqlmesh_config_dict = _build_sqlmesh_dict(final_sql_with_placeholders, args.gateway_name, args.state_db)
    try:
        with open(args.output, 'w') as f:
            yaml.dump(sqlmesh_config_dict, f, sort_keys=False, default_flow_style=False, indent=2)
        print(f"✅ Successfully generated SQLMesh config at: {args.output}")
    except Exception as e:
        print(f"❌ Failed to write output file: {e}")


def register_command(subparsers: _SubParsersAction):
    """Registers the command and its arguments to the main CLI parser."""
    parser_gen = subparsers.add_parser(
        "generate-sqlmesh-config",
        help="Generate a SQLMesh config file from a quackpipe config."
    )
    parser_gen.add_argument("-c", "--config", default="config.yml",
                            help="Path to the input quackpipe config.yml file. (Default: config.yml)")
    parser_gen.add_argument("-o", "--output", default="sqlmesh_config.yml",
                            help="Path for the output SQLMesh config file. (Default: sqlmesh_config.yml)")
    parser_gen.add_argument("--gateway-name", default="quackpipe_gateway",
                            help="The name for the gateway in the SQLMesh config. (Default: quackpipe_gateway)")
    parser_gen.add_argument("--state-db", default=".sqlmesh/state.db",
                            help="The path for the SQLMesh state database. (Default: .sqlmesh/state.db)")
    parser_gen.add_argument("--env-file", default=".env",
                            help="Path to the environment file to load secrets from. (Default: .env)")
    parser_gen.set_defaults(func=handler)

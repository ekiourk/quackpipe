"""Source Handler for S3 data sources."""
from .base import BaseSourceHandler
from typing import List, Dict, Any

from ..secrets import fetch_secret_bundle


class S3Handler(BaseSourceHandler):
    """
    Handler for S3 connections. Supports explicit credential creation via secrets
    or automatic detection (IAM/env vars) via SET commands.
    """

    @property
    def source_type(self):
        return "s3"

    @property
    def required_plugins(self) -> List[str]:
        return ["httpfs"]

    def render_sql(self, context: Dict[str, Any]) -> str:
        """
        Renders SQL to configure S3.
        - If a 'secret_name' is provided, it creates a named DuckDB secret.
        - Otherwise, it uses SET commands for session-local configuration,
          relying on DuckDB's automatic credential detection.
        """
        secret_name = context.get('secret_name')

        if secret_name:
            # Method 1: Explicit credentials using a named secret
            return self._render_create_secret_sql(context, secret_name)
        else:
            # Method 2: Session-local settings for automatic credential detection
            return self._render_set_commands_sql(context)

    def _render_create_secret_sql(self, context: Dict[str, Any], secret_name: str) -> str:
        """Builds a CREATE SECRET statement for S3."""
        secrets = fetch_secret_bundle(secret_name)
        sql_context = {**context, **secrets}

        # Mapping from our config keys to DuckDB's secret keys
        param_map = {
            'key_id': 'access_key_id',
            'secret': 'secret_access_key',
            'region': 'region',
            'session_token': 'session_token',
            'endpoint': 'endpoint',
            'url_style': 'url_style',
            'use_ssl': 'use_ssl'
        }

        parts = [f"CREATE OR REPLACE SECRET {context['connection_name']}_secret (", "  TYPE S3"]

        for duckdb_key, context_key in param_map.items():
            value = sql_context.get(context_key)
            if value is not None:
                # DuckDB expects string values in quotes, booleans/numbers are not.
                if isinstance(value, str):
                    parts.append(f",  {duckdb_key.upper()} '{value}'")
                else:  # Handle boolean or numeric values like use_ssl
                    parts.append(f",  {duckdb_key.upper()} {value}")

        parts.append(");")
        return "\n".join(parts)

    def _render_set_commands_sql(self, context: Dict[str, Any]) -> str:
        """Builds a series of SET commands for S3 configuration."""
        # Mapping from our config keys to DuckDB's SET variables
        param_map = {
            'region': 's3_region',
            'endpoint': 's3_endpoint',
            'url_style': 's3_url_style',
            'use_ssl': 's3_use_ssl'
            # We intentionally do not set s3_access_key_id or s3_secret_access_key
            # to allow DuckDB's automatic provider chain to work.
        }

        commands = []
        for context_key, duckdb_var in param_map.items():
            value = context.get(context_key)
            if value is not None:
                if isinstance(value, str):
                    commands.append(f"SET {duckdb_var} = '{value}';")
                else:
                    commands.append(f"SET {duckdb_var} = {value};")

        return "\n".join(commands)

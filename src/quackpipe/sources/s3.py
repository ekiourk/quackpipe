"""Source Handler for S3-compatible object storage."""
from typing import Any

from quackpipe.secrets import fetch_secret_bundle
from quackpipe.sources.base import BaseSourceHandler


class S3Handler(BaseSourceHandler):
    """
    Handler for S3 connections. Supports explicit credential creation via secrets
    or automatic detection (IAM/env vars) via SET commands.
    """
    def __init__(self, context: dict[str, Any]):
        super().__init__(context)

    @property
    def source_type(self):
        return "s3"

    @property
    def required_plugins(self) -> list[str]:
        return ["httpfs"]

    def render_create_secret_sql(self, duckdb_secret_name: str) -> str:
        """Builds a CREATE SECRET statement for S3."""
        secrets = fetch_secret_bundle(self.context.get('secret_name'))
        sql_context = {**self.context, **secrets}

        param_map = {
            'key_id': 'access_key_id', 'secret': 'secret_access_key',
            'region': 'region', 'session_token': 'session_token',
            'endpoint': 'endpoint', 'url_style': 'url_style', 'use_ssl': 'use_ssl'
        }

        parts = [f"CREATE OR REPLACE SECRET {duckdb_secret_name} (", "  TYPE S3"]

        for duckdb_key, context_key in param_map.items():
            value = sql_context.get(context_key)
            if value is not None:
                if isinstance(value, str):
                    parts.append(f",  {duckdb_key.upper()} '{value}'")
                else:
                    parts.append(f",  {duckdb_key.upper()} {value}")

        parts.append(");")
        return "\n".join(parts)

    def _render_set_commands_sql(self) -> str:
        """Builds a series of SET commands for S3 configuration."""
        param_map = {
            'access_key_id': 's3_access_key_id', 'secret_access_key': 's3_secret_access_key',
            'region': 's3_region', 'endpoint': 's3_endpoint',
            'url_style': 's3_url_style', 'use_ssl': 's3_use_ssl'
        }

        commands = []
        for context_key, duckdb_var in param_map.items():
            value = self.context.get(context_key)
            if value is not None:
                if isinstance(value, str):
                    commands.append(f"SET {duckdb_var} = '{value}';")
                else:
                    commands.append(f"SET {duckdb_var} = {value};")

        return "\n".join(commands)

    def render_sql(self) -> str:
        """
        Renders SQL to configure S3 based on the stored context.
        """
        secret_name = self.context.get('secret_name')

        if secret_name:
            duckdb_secret_name = f"{self.context['connection_name']}_secret"
            return self.render_create_secret_sql(duckdb_secret_name)
        else:
            return self._render_set_commands_sql()

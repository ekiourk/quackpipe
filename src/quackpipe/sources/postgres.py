"""Source Handler for PostgreSQL databases."""
from typing import Any

from quackpipe.secrets import fetch_secret_bundle
from quackpipe.sources.base import BaseSourceHandler


class PostgresHandler(BaseSourceHandler):
    """
    Handler for PostgreSQL connections using the 'postgres' extension.
    This handler uses the recommended CREATE SECRET + ATTACH pattern.
    """
    def __init__(self, context: dict[str, Any]):
        super().__init__(context)
        secrets = fetch_secret_bundle(self.context.get('secret_name'))
        self.context = {**self.context, **secrets}

    @property
    def source_type(self):
        return "postgres"

    @property
    def required_plugins(self) -> list[str]:
        return ["postgres"]

    def render_create_secret_sql(self, duckdb_secret_name: str) -> str:
        """Renders only the CREATE SECRET statement for Postgres."""

        secret_parts = [f"CREATE OR REPLACE SECRET {duckdb_secret_name} (", "  TYPE POSTGRES"]
        param_map = {'host': 'host', 'port': 'port', 'database': 'database', 'user': 'user', 'password': 'password'}

        for duckdb_key, context_key in param_map.items():
            value = self.context.get(context_key)
            if value is not None:
                if isinstance(value, str):
                    secret_parts.append(f",  {duckdb_key.upper()} '{value}'")
                else:
                    secret_parts.append(f",  {duckdb_key.upper()} {value}")
        secret_parts.append(");")
        return "\n".join(secret_parts)

    def _render_attach_sql(self, duckdb_secret_name: str) -> str:
        """Renders only the ATTACH and CREATE VIEW statements."""
        connection_name = self.context['connection_name']
        read_only_flag = ", READ_ONLY" if self.context.get('read_only', True) else ""

        attach_sql = (
            f"ATTACH 'dbname={self.context.get('database')}' AS {connection_name} "
            f"(TYPE POSTGRES, SECRET '{duckdb_secret_name}'{read_only_flag});"
        )

        view_sqls = []
        if 'tables' in self.context and isinstance(self.context['tables'], list):
            for table in self.context['tables']:
                view_name = f"{connection_name}_{table.replace('.', '_')}"
                view_sqls.append(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {connection_name}.{table};")

        return "\n".join([attach_sql] + view_sqls)

    def render_sql(self) -> str:
        """
        Renders the full SQL setup for a standalone Postgres connection.
        """
        duckdb_secret_name = f"{self.context['connection_name']}_secret"
        create_secret_sql = self.render_create_secret_sql(duckdb_secret_name)
        attach_sql = self._render_attach_sql(duckdb_secret_name)

        return "\n".join([create_secret_sql, attach_sql])

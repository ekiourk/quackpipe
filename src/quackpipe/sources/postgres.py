"""Source Handler for PostgreSQL databases."""
from typing import List, Dict, Any

from quackpipe.secrets import fetch_secret_bundle
from quackpipe.sources.base import BaseSourceHandler


class PostgresHandler(BaseSourceHandler):
    """
    Handler for PostgreSQL connections using the 'postgres' extension.
    This handler uses the recommended CREATE SECRET + ATTACH pattern.
    """
    def __init__(self, context: Dict[str, Any]):
        super().__init__(context)

    @property
    def source_type(self):
        return "postgres"

    @property
    def required_plugins(self) -> List[str]:
        return ["postgres"]

    def render_sql(self) -> str:
        """
        Renders SQL to create a named secret for Postgres credentials
        and then attaches the database by referencing that secret.
        """
        secrets = fetch_secret_bundle(self.context.get('secret_name'))
        sql_context = {**self.context, **secrets}

        connection_name = sql_context['connection_name']
        secret_name_for_duckdb = f"{connection_name}_secret"

        secret_parts = [f"CREATE OR REPLACE SECRET {secret_name_for_duckdb} (", "  TYPE POSTGRES"]
        param_map = {'host': 'host', 'port': 'port', 'database': 'database', 'user': 'user', 'password': 'password'}

        for duckdb_key, context_key in param_map.items():
            value = sql_context.get(context_key)
            if value is not None:
                if isinstance(value, str):
                    secret_parts.append(f",  {duckdb_key.upper()} '{value}'")
                else:
                    secret_parts.append(f",  {duckdb_key.upper()} {value}")
        secret_parts.append(");")
        create_secret_sql = "\n".join(secret_parts)

        read_only_flag = ", READ_ONLY" if sql_context.get('read_only', True) else ""

        attach_sql = (
            f"ATTACH 'dbname={sql_context.get('database')}' AS {connection_name} "
            f"(TYPE POSTGRES, SECRET '{secret_name_for_duckdb}'{read_only_flag});"
        )

        view_sqls = []
        if 'tables' in sql_context and isinstance(sql_context['tables'], list):
            for table in sql_context['tables']:
                view_name = f"{connection_name}_{table}"
                view_sqls.append(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {connection_name}.{table};")

        return "\n".join([create_secret_sql, attach_sql] + view_sqls)

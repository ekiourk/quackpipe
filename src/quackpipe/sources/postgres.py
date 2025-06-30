"""Source Handler for PostgreSQL databases."""
from .base import BaseSourceHandler
from ..secrets import fetch_secret_bundle
from typing import List, Dict, Any


class PostgresHandler(BaseSourceHandler):
    """
    Handler for PostgreSQL connections using the 'postgres' extension.
    This handler uses the recommended CREATE SECRET + ATTACH pattern.
    """

    @property
    def source_type(self):
        return "postgres"

    @property
    def required_plugins(self) -> List[str]:
        return ["postgres"]

    def render_sql(self, context: Dict[str, Any]) -> str:
        """
        Renders SQL to create a named secret for Postgres credentials
        and then attaches the database by referencing that secret.
        """
        secrets = fetch_secret_bundle(context.get('secret_name'))
        sql_context = {**context, **secrets}

        connection_name = sql_context['connection_name']
        secret_name_for_duckdb = f"{connection_name}_secret"

        # 1. Build the CREATE SECRET statement dynamically
        secret_parts = [f"CREATE OR REPLACE SECRET {secret_name_for_duckdb} (", "  TYPE POSTGRES"]
        param_map = {
            'host': 'host',
            'port': 'port',
            'database': 'database',
            'user': 'user',
            'password': 'password'
        }
        for duckdb_key, context_key in param_map.items():
            value = sql_context.get(context_key)
            if value is not None:
                if isinstance(value, str):
                    secret_parts.append(f",  {duckdb_key.upper()} '{value}'")
                else:
                    secret_parts.append(f",  {duckdb_key.upper()} {value}")
        secret_parts.append(");")
        create_secret_sql = "\n".join(secret_parts)

        # 2. Build the ATTACH statement referencing the secret
        # The READ_ONLY flag is present if true, and absent if false.
        read_only_flag = ", READ_ONLY" if sql_context.get('read_only', True) else ""

        attach_sql = (
            f"ATTACH 'dbname={sql_context.get('database')}' AS {connection_name} "
            f"(TYPE POSTGRES, SECRET '{secret_name_for_duckdb}'{read_only_flag});"
        )

        # 3. Build CREATE VIEW statements if tables are specified
        view_sqls = []
        if 'tables' in sql_context and isinstance(sql_context['tables'], list):
            for table in sql_context['tables']:
                view_name = f"{connection_name}_{table}"
                view_sqls.append(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {connection_name}.{table};")

        # Combine all SQL statements
        return "\n".join([create_secret_sql, attach_sql] + view_sqls)


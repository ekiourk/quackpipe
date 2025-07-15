"""Source Handler for Azure Blob Storage."""
from typing import Any

from quackpipe.secrets import fetch_secret_bundle
from quackpipe.sources.base import BaseSourceHandler


class AzureBlobHandler(BaseSourceHandler):
    """
    Handler for Azure Blob Storage connections using the 'azure' extension.
    This handler uses the CREATE SECRET pattern for authentication.
    """

    def __init__(self, context: dict[str, Any]):
        super().__init__(context)

    @property
    def source_type(self) -> str:
        return "azure"

    @property
    def required_plugins(self) -> list[str]:
        return ["azure", "httpfs"]

    def render_sql(self) -> str:
        """
        Renders the CREATE SECRET statement for Azure Blob Storage.
        """
        secrets = fetch_secret_bundle(self.context.get('secret_name'))
        sql_context = {**self.context, **secrets}

        connection_name = sql_context.get('connection_name')
        duckdb_secret_name = f"{connection_name}_secret"

        # The 'provider' determines the authentication method
        provider = sql_context.get('provider', 'connection_string').lower()

        secret_parts = [f"CREATE OR REPLACE SECRET {duckdb_secret_name} (", "  TYPE AZURE"]

        if provider == 'connection_string':
            # This is the simplest method, using a single connection string
            connection_string = sql_context.get('connection_string')
            if not connection_string:
                raise ValueError(
                    f"Azure source '{connection_name}' with provider 'connection_string' requires a 'connection_string' parameter.")
            secret_parts.append(f",  CONNECTION_STRING '{connection_string}'")

        elif provider == 'service_principal':
            # Using a Service Principal (app registration)
            param_map = {
                'account': 'account',
                'tenant_id': 'tenant_id',
                'client_id': 'client_id',
                'client_secret': 'client_secret'
            }
            secret_parts.append(",  PROVIDER 'service_principal'")
            for duckdb_key, context_key in param_map.items():
                value = sql_context.get(context_key)
                if value:
                    secret_parts.append(f",  {duckdb_key.upper()} '{value}'")

        elif provider == 'managed_identity':
            # Using a Managed Identity
            secret_parts.append(",  PROVIDER 'credential_chain'")
            if 'account' in sql_context:
                secret_parts.append(f",  ACCOUNT '{sql_context['account']}'")

        else:
            raise ValueError(
                f"Unsupported Azure provider type: '{provider}'. Must be 'connection_string', 'service_principal', or 'managed_identity'.")

        secret_parts.append(");")

        # The Azure extension works by setting the secret for the session.
        # No ATTACH statement is needed; users will query using full paths like 'azure://container/file.parquet'.
        return "\n".join(secret_parts)

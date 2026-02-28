"""Source Handler for Azure Blob Storage."""
from typing import Any

from quackpipe.exceptions import ValidationError
from quackpipe.secrets import fetch_secret_bundle
from quackpipe.sources.base import BaseSourceHandler
from quackpipe.validation_utils import get_merged_params, validate_required_fields


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

    @classmethod
    def validate(cls, config: dict[str, Any], secret_name: str | None = None, resolve_secrets: bool = False):
        """Validates Azure Blob Storage configuration parameters."""
        params = get_merged_params(config, secret_name, resolve_secrets)
        provider = params.get('provider', 'connection_string').lower()

        if provider == 'connection_string':
            validate_required_fields(params, ['connection_string'], "azure", secret_name, resolve_secrets)
        elif provider == 'service_principal':
            required = ['account_name', 'tenant_id', 'client_id', 'client_secret']
            validate_required_fields(params, required, "azure", secret_name, resolve_secrets)
        elif provider == 'managed_identity':
            pass # No specific fields required for managed identity
        else:
            raise ValidationError(f"Unsupported Azure provider type: '{provider}'. Must be 'connection_string', 'service_principal', or 'managed_identity'.")

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
            secret_parts.append(f",  CONNECTION_STRING '{connection_string}'")

        elif provider == 'service_principal':
            # Using a Service Principal (app registration)
            param_map = {
                'account_name': 'account_name',
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
            if 'account_name' in sql_context:
                secret_parts.append(f",  ACCOUNT_NAME '{sql_context['account_name']}'")

        else:
            # Defensive check: should be unreachable due to validate()
            raise ValidationError(f"Unsupported Azure provider type in render_sql: '{provider}'")

        secret_parts.append(");")

        # The Azure extension works by setting the secret for the session.
        # No ATTACH statement is needed; users will query using full paths like 'azure://container/file.parquet'.
        return "\n".join(secret_parts)

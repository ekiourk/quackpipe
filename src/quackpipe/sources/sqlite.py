"""Source Handler for SQLite databases."""
from typing import Any

from quackpipe.sources.base import BaseSourceHandler
from quackpipe.validation_utils import get_merged_params, validate_required_fields


class SQLiteHandler(BaseSourceHandler):
    """
    Handler for SQLite database connections using the 'sqlite' extension.
    """

    def __init__(self, context: dict[str, Any]):
        super().__init__(context)

    @property
    def source_type(self) -> str:
        return "sqlite"

    @property
    def required_plugins(self) -> list[str]:
        return ["sqlite"]

    @classmethod
    def validate(cls, config: dict[str, Any], secret_name: str | None = None, resolve_secrets: bool = False):
        """Validates SQLite configuration parameters."""
        params = get_merged_params(config, secret_name, resolve_secrets)
        validate_required_fields(params, ["path"], "sqlite", secret_name, resolve_secrets)

    def render_sql(self) -> str:
        """
        Renders the ATTACH statement for a SQLite database file.
        """
        connection_name = self.context.get('connection_name')
        db_path = self.context.get('path')

        # The READ_ONLY flag is present if true, and absent if false.
        read_only_flag = ", READ_ONLY" if self.context.get('read_only', True) else ""

        # Handle native encryption (DuckDB 1.4+)
        encryption_key = self.context.get('encryption_key')
        encryption_flag = f", ENCRYPTION_KEY '{encryption_key}'" if encryption_key else ""

        # Build the ATTACH statement
        attach_sql = (
            f"ATTACH '{db_path}' AS {connection_name} "
            f"(TYPE SQLITE{read_only_flag}{encryption_flag});"
        )

        return attach_sql

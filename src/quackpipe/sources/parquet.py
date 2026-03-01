"""Source Handler for Parquet files."""

from typing import Any

from quackpipe.sources.base import BaseSourceHandler
from quackpipe.validation_utils import get_merged_params, validate_required_fields


class ParquetHandler(BaseSourceHandler):
    """
    Handler for Parquet files.
    """

    def __init__(self, context: dict[str, Any]):
        super().__init__(context)

    @property
    def source_type(self) -> str:
        return "parquet"

    @property
    def required_plugins(self) -> list[str]:
        return []  # Parquet is built-in to DuckDB

    @classmethod
    def validate(cls, config: dict[str, Any], secret_name: str | None = None, resolve_secrets: bool = False):
        """Validates Parquet configuration parameters."""
        params = get_merged_params(config, secret_name, resolve_secrets)
        validate_required_fields(params, ["path"], "parquet", secret_name, resolve_secrets)

    def render_sql(self) -> str:
        """
        Renders a VIEW for the Parquet file.
        """
        connection_name = self.context.get("connection_name")
        path = self.context.get("path")

        return f"CREATE OR REPLACE VIEW {connection_name} AS SELECT * FROM read_parquet('{path}');"

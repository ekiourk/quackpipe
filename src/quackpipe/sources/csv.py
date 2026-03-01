"""Source Handler for CSV files."""

from typing import Any

from quackpipe.sources.base import BaseSourceHandler
from quackpipe.validation_utils import get_merged_params, validate_required_fields


class CSVHandler(BaseSourceHandler):
    """
    Handler for CSV files.
    """

    def __init__(self, context: dict[str, Any]) -> None:
        super().__init__(context)

    @property
    def source_type(self) -> str:
        return "csv"

    @property
    def required_plugins(self) -> list[str]:
        return []  # CSV is built-in to DuckDB

    @classmethod
    def validate(cls, config: dict[str, Any], secret_name: str | None = None, resolve_secrets: bool = False) -> None:
        """Validates CSV configuration parameters."""
        params = get_merged_params(config, secret_name, resolve_secrets)
        validate_required_fields(params, ["path"], "csv", secret_name, resolve_secrets)

    def render_sql(self) -> str:
        """
        Renders a VIEW for the CSV file.
        """
        connection_name = self.context.get("connection_name")
        path = self.context.get("path")

        return f"CREATE OR REPLACE VIEW {connection_name} AS SELECT * FROM read_csv_auto('{path}');"

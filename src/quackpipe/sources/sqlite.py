"""Source Handler for SQLite databases."""
from typing import Any

from quackpipe.sources.base import BaseSourceHandler


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

    def render_sql(self) -> str:
        """
        Renders the ATTACH statement for a SQLite database file.
        """
        connection_name = self.context.get('connection_name')
        db_path = self.context.get('path')

        if not db_path:
            raise ValueError(f"SQLite source '{connection_name}' requires a 'path' in its configuration.")

        # The READ_ONLY flag is present if true, and absent if false.
        read_only_flag = ", READ_ONLY" if self.context.get('read_only', True) else ""

        # Build the ATTACH statement
        attach_sql = (
            f"ATTACH '{db_path}' AS {connection_name} "
            f"(TYPE SQLITE{read_only_flag});"
        )

        return attach_sql

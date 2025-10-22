"""
General utility functions for the quackpipe library.
"""
from collections import OrderedDict

from duckdb import ConnectionException, DuckDBPyConnection


class DotDict(OrderedDict):
    """A dict subclass that allows dot notation access to dictionary keys.
    All keys are automatically converted to lowercase."""

    def __setitem__(self, key, value):
        super().__setitem__(key.lower() if isinstance(key, str) else key, value)

    def __getitem__(self, key):
        return super().__getitem__(key.lower() if isinstance(key, str) else key)

    def __delitem__(self, key):
        super().__delitem__(key.lower() if isinstance(key, str) else key)

    def __contains__(self, key):
        return super().__contains__(key.lower() if isinstance(key, str) else key)

    def get(self, key, default=None):
        return super().get(key.lower() if isinstance(key, str) else key, default)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{key}'") from None

    def __setattr__(self, key, value):
        if key.startswith('_'):
            # Allow OrderedDict internal attributes
            super(OrderedDict, self).__setattr__(key, value)
        else:
            self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{key}'") from None


def is_connection_open(conn: DuckDBPyConnection) -> bool:
    try:
        conn.execute("SELECT 1")
        return True
    except ConnectionException:
        return False

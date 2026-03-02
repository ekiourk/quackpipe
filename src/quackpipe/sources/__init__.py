"""
Source handlers for different data source types.
"""

from quackpipe.config import SourceType

from . import azure_blob, csv, ducklake, mysql, parquet, postgres, s3, sqlite
from .base import BaseSourceHandler

SOURCE_HANDLER_REGISTRY: dict[SourceType, type[BaseSourceHandler]] = {
    SourceType.POSTGRES: postgres.PostgresHandler,
    SourceType.MYSQL: mysql.MySQLHandler,
    SourceType.S3: s3.S3Handler,
    SourceType.AZURE: azure_blob.AzureBlobHandler,
    SourceType.DUCKLAKE: ducklake.DuckLakeHandler,
    SourceType.SQLITE: sqlite.SQLiteHandler,
    SourceType.PARQUET: parquet.ParquetHandler,
    SourceType.CSV: csv.CSVHandler,
}

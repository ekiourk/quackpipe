import logging
from collections.abc import Generator
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import create_engine
from testcontainers.postgres import PostgresContainer

from quackpipe import QuackpipeBuilder, SourceType
from quackpipe.test_utils.data_generators import (
    create_employee_data,
    create_monthly_data,
    create_vessel_definitions,
    generate_synthetic_ais_data,
)

logger = logging.getLogger(__name__)

POSTGRES_SOURCE_NAME = "pg_source"


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, Any, None]:
    container = PostgresContainer("postgres:15-alpine")
    container.with_env("POSTGRES_USER", "test")
    container.with_env("POSTGRES_PASSWORD", "test")
    container.with_env("POSTGRES_DB", "test")
    container.dbname = "test"
    with container as postgres:
        yield postgres


@pytest.fixture(scope="module")
def postgres_engine(postgres_container):
    """Returns a SQLAlchemy engine for the PostgreSQL container."""
    return create_engine(postgres_container.get_connection_url())


@pytest.fixture(scope="module")
def postgres_container_with_data(postgres_container, quackpipe_with_pg_source) -> PostgresContainer:
    """
    Starts a PostgreSQL container and populates it with sample data for testing
    using a temporary Quackpipe connection.
    """

    employee_data = create_employee_data()
    monthly_data = create_monthly_data()
    vessels = create_vessel_definitions()

    employees_df = pd.DataFrame(employee_data)
    monthly_df = pd.DataFrame(monthly_data)
    vessels_df = pd.DataFrame(vessels)
    synthetic_ais_df = generate_synthetic_ais_data(vessels)
    synthetic_ais_df['BaseDateTime'] = pd.to_datetime(synthetic_ais_df['BaseDateTime'])
    synthetic_ais_df.columns = synthetic_ais_df.columns.str.lower()

    with quackpipe_with_pg_source.session() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SOURCE_NAME}.company;")
        con.execute(f"CREATE TABLE {POSTGRES_SOURCE_NAME}.company.employees AS SELECT * FROM employees_df;")
        con.execute(f"CREATE TABLE {POSTGRES_SOURCE_NAME}.company.monthly_reports AS SELECT * FROM monthly_df;")
        con.execute(f"CREATE TABLE {POSTGRES_SOURCE_NAME}.vessels AS SELECT * FROM vessels_df;")
        con.execute(f"CREATE TABLE {POSTGRES_SOURCE_NAME}.ais_data AS SELECT * FROM synthetic_ais_df;")

        con.execute(f"CREATE INDEX idx_employees_department ON {POSTGRES_SOURCE_NAME}.company.employees(department);")
        con.execute(f"CREATE INDEX idx_ais_mmsi ON {POSTGRES_SOURCE_NAME}.ais_data(mmsi);")
        con.execute(f"CREATE INDEX idx_ais_datetime ON {POSTGRES_SOURCE_NAME}.ais_data(basedatetime);")
        con.execute(f"CREATE INDEX idx_vessels_mmsi ON {POSTGRES_SOURCE_NAME}.vessels(mmsi);")

        # Use postgres_execute to create the view directly in PostgreSQL
        create_view_sql = """
            CREATE VIEW ais_with_vessel_info AS
            SELECT a.*,
                   v.name   as vessel_name_from_vessels,
                   v.type   as vessel_type_from_vessels,
                   v.length as vessel_length_from_vessels,
                   v.width  as vessel_width_from_vessels
            FROM ais_data a
            LEFT JOIN vessels v ON a.mmsi = v.mmsi;
        """
        con.execute(f"CALL postgres_execute('{POSTGRES_SOURCE_NAME}', '{create_view_sql.replace('\'', '''''')}')")

    logger.info("PostgreSQL container populated via Quackpipe with:")
    logger.info(f"  - {len(employees_df)} employee records")
    logger.info(f"  - {len(monthly_df)} monthly report records")
    logger.info(f"  - {len(vessels_df)} vessel definitions")
    logger.info(f"  - {len(synthetic_ais_df)} AIS data records")
    logger.info("  - Created indexes and views for better query performance")

    return postgres_container


@pytest.fixture(scope="module")
def quackpipe_with_pg_source(postgres_container) -> QuackpipeBuilder:
    """
    Provides a Quackpipe builder with a read-write connection to the PostgreSQL source.
    """
    builder = QuackpipeBuilder().add_source(
        name=POSTGRES_SOURCE_NAME,
        type=SourceType.POSTGRES,
        config={
            'database': 'test',
            'user': 'test',
            'password': 'test',
            'host': postgres_container.get_container_host_ip(),
            'port': postgres_container.get_exposed_port(5432),
            'connection_name': 'pg_main',
            'read_only': False
        }
    )
    return builder

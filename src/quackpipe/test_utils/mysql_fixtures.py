import logging

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from testcontainers.mysql import MySqlContainer

from quackpipe import QuackpipeBuilder, SourceType
from quackpipe.test_utils.data_generators import (
    create_employee_data,
    create_monthly_data,
    create_vessel_definitions,
    generate_synthetic_ais_data,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def mysql_container():
    container = MySqlContainer("mysql:8.3.0", dialect="pymysql")
    container.with_env("MYSQL_USER", "test")
    container.with_env("MYSQL_PASSWORD", "test")
    container.with_env("MYSQL_DATABASE", "test")
    container.with_env("MYSQL_ROOT_PASSWORD", "test")
    container.dbname = "test"
    with container as mysql:
        yield mysql


@pytest.fixture(scope="module")
def mysql_engine(mysql_container):
    """Returns a SQLAlchemy engine for the MySQL container."""
    return create_engine(mysql_container.get_connection_url())


@pytest.fixture(scope="module")
def mysql_container_with_data(mysql_container, mysql_engine):
    """
    Starts a MySQL container with sample data for testing.
    Creates tables and populates them with synthetic data.
    """

    employee_data = create_employee_data()
    monthly_data = create_monthly_data()
    vessels = create_vessel_definitions()

    # Create DataFrames
    employees_df = pd.DataFrame(employee_data)
    monthly_df = pd.DataFrame(monthly_data)
    synthetic_ais_df = generate_synthetic_ais_data(vessels)

    # Create tables and insert data
    with mysql_engine.connect() as conn:
        # MySQL doesn't have schemas in the same way as postgres, so we'll just use tables.
        # However, we can create a separate database for the company data if needed, but for now, we'll use tables.
        # For simplicity, we'll create tables with prefixes or just simple names.
        employees_df.to_sql('company_employees', conn, if_exists='replace', index=False)
        monthly_df.to_sql('company_monthly_reports', conn, if_exists='replace', index=False)

        vessels_df = pd.DataFrame(vessels)
        vessels_df.to_sql('vessels', conn, if_exists='replace', index=False)

        ais_df_mysql = synthetic_ais_df.copy()
        ais_df_mysql['BaseDateTime'] = pd.to_datetime(ais_df_mysql['BaseDateTime'])
        ais_df_mysql.columns = ais_df_mysql.columns.str.lower()
        ais_df_mysql.to_sql('ais_data', conn, if_exists='replace', index=False)

        conn.execute(text("CREATE INDEX idx_ais_mmsi ON ais_data(mmsi)"))
        conn.execute(text("CREATE INDEX idx_ais_datetime ON ais_data(basedatetime)"))
        conn.execute(text("CREATE INDEX idx_vessels_mmsi ON vessels(mmsi)"))

        conn.execute(text("""
                          CREATE VIEW ais_with_vessel_info AS
                          SELECT a.*,
                                 v.name   as vessel_name_from_vessels,
                                 v.type   as vessel_type_from_vessels,
                                 v.length as vessel_length_from_vessels,
                                 v.width  as vessel_width_from_vessels
                          FROM ais_data a
                                   LEFT JOIN vessels v ON a.mmsi = v.mmsi
                          """))
        conn.commit()

    logger.info("MySQL container populated with:")
    logger.info(f"  - {len(employees_df)} employee records")
    logger.info(f"  - {len(monthly_df)} monthly report records")
    logger.info(f"  - {len(vessels_df)} vessel definitions")
    logger.info(f"  - {len(synthetic_ais_df)} AIS data records")
    logger.info("  - Created indexes and views for better query performance")

    return mysql_container


@pytest.fixture(scope="module")
def quackpipe_with_mysql_source(mysql_container_with_data) -> QuackpipeBuilder:
    builder = QuackpipeBuilder().add_source(
        name="mysql_source",
        type=SourceType.MYSQL,
        config={
            'database': 'test',
            'user': 'test',
            'password': 'test',
            'host': mysql_container_with_data.get_container_host_ip(),
            'port': mysql_container_with_data.get_exposed_port(3306),
            'connection_name': 'mysql_main',
            'read_only': True,
            'tables': ['company_employees', 'company_monthly_reports', 'vessels']
        }
    )
    return builder

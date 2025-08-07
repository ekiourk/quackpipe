import logging

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from quackpipe import QuackpipeBuilder, SourceType
from quackpipe.test_utils.data_generators import (
    create_employee_data,
    create_monthly_data,
    create_vessel_definitions,
    generate_synthetic_ais_data,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:15-alpine", username="test", password="test", dbname="test") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def postgres_engine(postgres_container):
    """Returns a SQLAlchemy engine for the PostgreSQL container."""
    return create_engine(postgres_container.get_connection_url())


@pytest.fixture(scope="module")
def postgres_container_with_data(postgres_container, postgres_engine):
    """
    Starts a PostgreSQL container with sample data for testing.
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
    with postgres_engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA company"))
        # Create and populate employees table
        employees_df.to_sql('employees', conn, schema='company', if_exists='replace', index=False)

        # Create and populate monthly_reports table
        monthly_df.to_sql('monthly_reports', conn, schema='company', if_exists='replace', index=False)

        # Create and populate vessels table (from vessel definitions)
        vessels_df = pd.DataFrame(vessels)
        vessels_df.to_sql('vessels', conn, if_exists='replace', index=False)

        # Create and populate AIS data table
        # Note: Converting BaseDateTime to proper datetime for PostgreSQL
        ais_df_pg = synthetic_ais_df.copy()
        ais_df_pg['BaseDateTime'] = pd.to_datetime(ais_df_pg['BaseDateTime'])

        # Convert column names to lowercase for PostgreSQL
        ais_df_pg.columns = ais_df_pg.columns.str.lower()
        ais_df_pg.to_sql('ais_data', conn, if_exists='replace', index=False)

        # Create some indexes for better query performance
        conn.execute(text("CREATE INDEX idx_employees_department ON company.employees(department)"))
        conn.execute(text("CREATE INDEX idx_ais_mmsi ON ais_data(mmsi)"))
        conn.execute(text("CREATE INDEX idx_ais_datetime ON ais_data(basedatetime)"))
        conn.execute(text("CREATE INDEX idx_vessels_mmsi ON vessels(mmsi)"))

        # Create a view that joins AIS data with vessel information
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

    logger.info("PostgreSQL container populated with:")
    logger.info(f"  - {len(employees_df)} employee records")
    logger.info(f"  - {len(monthly_df)} monthly report records")
    logger.info(f"  - {len(vessels_df)} vessel definitions")
    logger.info(f"  - {len(synthetic_ais_df)} AIS data records")
    logger.info("  - Created indexes and views for better query performance")

    return postgres_container


@pytest.fixture(scope="module")
def quackpipe_with_pg_source(postgres_container_with_data) -> QuackpipeBuilder:
    builder = QuackpipeBuilder().add_source(
        name="pg_source",
        type=SourceType.POSTGRES,
        config={
            'database': 'test',
            'user': 'test',
            'password': 'test',
            'host': postgres_container_with_data.get_container_host_ip(),
            'port': postgres_container_with_data.get_exposed_port(5432),
            'connection_name': 'pg_main',
            'read_only': True,
            'tables': ['company.employees', 'company.monthly_reports', 'vessels']
        }
    )
    return builder

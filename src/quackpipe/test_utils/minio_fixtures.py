import io
import logging

import pandas as pd
import pytest
from testcontainers.minio import MinioContainer

from quackpipe import QuackpipeBuilder, SourceType
from quackpipe.test_utils.data_generators import (
    create_ais_summary,
    create_employee_data,
    create_monthly_data,
    create_vessel_definitions,
    generate_synthetic_ais_data,
)

logger = logging.getLogger(__name__)

TEST_BUCKET_NAME = "test-bucket"

@pytest.fixture(scope="module")
def minio_container():
    """Starts a MinIO container."""
    with MinioContainer("minio/minio:RELEASE.2025-06-13T11-33-47Z") as minio:
        # It's good practice to create the bucket ahead of time.
        minio.get_client().make_bucket(TEST_BUCKET_NAME)
        yield minio


@pytest.fixture(scope="module")
def quackpipe_with_minio(minio_container):
    builder = QuackpipeBuilder().add_source(
        name=TEST_BUCKET_NAME,
        type=SourceType.S3,
        config={
            "path": f"s3://{TEST_BUCKET_NAME}/",
            "endpoint": minio_container.get_config()["endpoint"],
            "access_key_id": minio_container.access_key,
            "secret_access_key": minio_container.secret_key,
            "use_ssl": False,
            "url_style": "path"
        }
    )
    return builder


@pytest.fixture(scope="module")
def minio_container_with_data(minio_container):
    """
    Starts a MinIO container with sample data for testing.
    Creates a bucket with example CSV and Parquet files.
    test-lake/
    ├── data/
    │   ├── employees.csv
    │   ├── employees.parquet
    │   └── monthly_reports.csv
    ├── partitioned/
    │   ├── department=Engineering/employees.csv
    │   ├── department=Marketing/employees.csv
    │   └── department=Sales/employees.csv
    └── external/
        ├── ais_data_synthetic.csv
        ├── ais_data_synthetic.parquet
        └── ais_data_summary.json
    """

    client = minio_container.get_client()

    # Generate all datasets
    employee_data = create_employee_data()
    monthly_data = create_monthly_data()
    vessels = create_vessel_definitions()

    # Create DataFrames
    df = pd.DataFrame(employee_data)
    monthly_df = pd.DataFrame(monthly_data)

    # Upload employee CSV file
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue().encode('utf-8')

    client.put_object(
        bucket_name=TEST_BUCKET_NAME,
        object_name="data/employees.csv",
        data=io.BytesIO(csv_data),
        length=len(csv_data),
        content_type="text/csv"
    )

    # Upload employee Parquet file
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_data = parquet_buffer.getvalue()

    client.put_object(
        bucket_name=TEST_BUCKET_NAME,
        object_name="data/employees.parquet",
        data=io.BytesIO(parquet_data),
        length=len(parquet_data),
        content_type="application/octet-stream"
    )

    # Upload monthly CSV
    monthly_csv_buffer = io.StringIO()
    monthly_df.to_csv(monthly_csv_buffer, index=False)
    monthly_csv_data = monthly_csv_buffer.getvalue().encode('utf-8')

    client.put_object(
        bucket_name=TEST_BUCKET_NAME,
        object_name="data/monthly_reports.csv",
        data=io.BytesIO(monthly_csv_data),
        length=len(monthly_csv_data),
        content_type="text/csv"
    )

    # Create partitioned data
    for dept in ['Engineering', 'Marketing', 'Sales']:
        dept_data = df[df['department'] == dept].copy()
        if not dept_data.empty:
            dept_csv_buffer = io.StringIO()
            dept_data.to_csv(dept_csv_buffer, index=False)
            dept_csv_data = dept_csv_buffer.getvalue().encode('utf-8')

            client.put_object(
                bucket_name=TEST_BUCKET_NAME,
                object_name=f"partitioned/department={dept}/employees.csv",
                data=io.BytesIO(dept_csv_data),
                length=len(dept_csv_data),
                content_type="text/csv"
            )

    # Generate synthetic AIS data
    logger.info("Creating synthetic AIS data...")
    synthetic_ais_df = generate_synthetic_ais_data(vessels)

    # Upload synthetic CSV
    synthetic_csv_buffer = io.StringIO()
    synthetic_ais_df.to_csv(synthetic_csv_buffer, index=False)
    synthetic_csv_data = synthetic_csv_buffer.getvalue().encode('utf-8')

    client.put_object(
        bucket_name=TEST_BUCKET_NAME,
        object_name="external/ais_data_synthetic.csv",
        data=io.BytesIO(synthetic_csv_data),
        length=len(synthetic_csv_data),
        content_type="text/csv"
    )

    # Upload synthetic Parquet
    synthetic_parquet_buffer = io.BytesIO()
    synthetic_ais_df.to_parquet(synthetic_parquet_buffer, index=False)
    synthetic_parquet_data = synthetic_parquet_buffer.getvalue()

    client.put_object(
        bucket_name=TEST_BUCKET_NAME,
        object_name="external/ais_data_synthetic.parquet",
        data=io.BytesIO(synthetic_parquet_data),
        length=len(synthetic_parquet_data),
        content_type="application/octet-stream"
    )

    # Create and upload AIS summary
    ais_summary = create_ais_summary(synthetic_ais_df, vessels)
    summary_json = pd.Series(ais_summary).to_json(indent=2)

    client.put_object(
        bucket_name=TEST_BUCKET_NAME,
        object_name="external/ais_data_summary.json",
        data=io.BytesIO(summary_json.encode('utf-8')),
        length=len(summary_json.encode('utf-8')),
        content_type="application/json"
    )

    logger.info(f"Successfully created synthetic AIS data with {len(synthetic_ais_df)} records")
    logger.info("AIS data setup complete!")

    return minio_container

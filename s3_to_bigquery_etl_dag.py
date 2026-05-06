"""
S3 TO GCS TO BIGQUERY ETL PIPELINE
=================================

Purpose:
    This DAG demonstrates a real-world ETL pipeline using:
    - AWS S3
    - Google Cloud Storage (GCS)
    - BigQuery
    - Apache Airflow

Pipeline Flow:
    AWS S3 Bucket (orders.csv)
            ↓
    Airflow Orchestration
            ↓
    Copy file from S3 to GCS
            ↓
    Load data into BigQuery raw table
            ↓
    Run SQL transformation
            ↓
    Create final reporting table

Important Airflow Concepts Used:
    - DAG: Defines the complete workflow
    - Task: A single unit of work
    - Operator: Defines what each task does
    - Dependency: Defines task execution order
    - Retry: Retries failed tasks based on configuration
"""

# -----------------------------------------------------------------------------
# 1. IMPORTS
# -----------------------------------------------------------------------------
# Imports bring required Airflow classes and Python modules into this file.

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

# S3ToGCSOperator copies files from AWS S3 to Google Cloud Storage.
from airflow.providers.amazon.aws.transfers.s3_to_gcs import S3ToGCSOperator

# GCSToBigQueryOperator loads files from Google Cloud Storage into BigQuery.
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# BigQueryInsertJobOperator runs SQL queries/jobs in BigQuery.
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# -----------------------------------------------------------------------------
# 2. VARIABLES / CONFIGURATION
# -----------------------------------------------------------------------------
# These variables make the DAG easier to maintain.
# Replace placeholder values with actual project/bucket names in real usage.

PROJECT_ID = "your-gcp-project-id"
LOCATION = "US"

# AWS S3 configuration
AWS_S3_BUCKET = "your-s3-bucket-name"
S3_SOURCE_PATH = "orders/orders.csv"

# GCS configuration
GCS_BUCKET = "your-gcs-bucket-name"
GCS_DESTINATION_PATH = "landing/orders.csv"

# BigQuery datasets
RAW_DATASET = "raw_ds"
FINAL_DATASET = "insight_ds"

# BigQuery table names
RAW_TABLE = "orders_raw"
FINAL_TABLE = "orders_summary"


# -----------------------------------------------------------------------------
# 3. DEFAULT ARGUMENTS
# -----------------------------------------------------------------------------
# default_args controls common DAG/task behavior like owner, start date,
# retry count, retry delay, and notification settings.

ARGS = {
    "owner": "data_engineer",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# -----------------------------------------------------------------------------
# 4. SQL TRANSFORMATION QUERY
# -----------------------------------------------------------------------------
# This query creates or replaces the final reporting table.
# It performs basic transformation on raw order data.

QUERY = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{FINAL_DATASET}.{FINAL_TABLE}` AS
SELECT
    OrderID,
    CustomerID,
    ProductCategory,
    Quantity,
    Amount,
    Quantity * Amount AS TotalAmount,
    OrderDate
FROM `{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}`
WHERE OrderID IS NOT NULL
"""


# -----------------------------------------------------------------------------
# 5. DAG DEFINITION
# -----------------------------------------------------------------------------
# DAG represents the full workflow.
# schedule_interval = "0 6 * * *" means this DAG runs daily at 6 AM.

with DAG(
    dag_id="S3_TO_BIGQUERY_ETL_DAG",
    schedule_interval="0 6 * * *",
    description="Load data from AWS S3 to BigQuery using GCS staging and Airflow",
    default_args=ARGS,
    tags=["airflow", "aws", "s3", "gcs", "bigquery", "etl"],
) as dag:

    # -------------------------------------------------------------------------
    # 6. TASK 1 - COPY FILE FROM S3 TO GCS
    # -------------------------------------------------------------------------
    # This task copies orders.csv from AWS S3 into Google Cloud Storage.
    # Many enterprise pipelines use GCS as a staging layer before BigQuery load.

    task1 = S3ToGCSOperator(
        task_id="copy_s3_to_gcs",
        bucket=AWS_S3_BUCKET,
        prefix=S3_SOURCE_PATH,
        dest_gcs=f"gs://{GCS_BUCKET}/landing/",
        replace=True,
    )

    # -------------------------------------------------------------------------
    # 7. TASK 2 - LOAD FILE FROM GCS TO BIGQUERY RAW TABLE
    # -------------------------------------------------------------------------
    # This task loads orders.csv from GCS into a BigQuery raw/staging table.
    # WRITE_TRUNCATE means the table is overwritten every time the DAG runs.

    task2 = GCSToBigQueryOperator(
        task_id="load_orders_raw",
        bucket=GCS_BUCKET,
        source_objects=[GCS_DESTINATION_PATH],
        destination_project_dataset_table=f"{RAW_DATASET}.{RAW_TABLE}",
        schema_fields=[
            {"name": "OrderID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "CustomerID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "ProductCategory", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Quantity", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Amount", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "OrderDate", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        skip_leading_rows=1,
    )

    # -------------------------------------------------------------------------
    # 8. TASK 3 - RUN BIGQUERY SQL TRANSFORMATION
    # -------------------------------------------------------------------------
    # This task runs a BigQuery SQL transformation to create the final table.

    task3 = BigQueryInsertJobOperator(
        task_id="transform_orders",
        configuration={
            "query": {
                "query": QUERY,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    # -------------------------------------------------------------------------
    # 9. TASK DEPENDENCIES
    # -------------------------------------------------------------------------
    # task1 >> task2 >> task3 means:
    # 1. Copy file from S3 to GCS
    # 2. Load file from GCS to BigQuery raw table
    # 3. Run SQL transformation

    task1 >> task2 >> task3


# -----------------------------------------------------------------------------
# INTERVIEW EXPLANATION
# -----------------------------------------------------------------------------
# This DAG demonstrates a common enterprise ETL pattern where data is copied
# from AWS S3 into Google Cloud Storage, then loaded into BigQuery raw tables.
# After the raw load is complete, Airflow runs a BigQuery SQL transformation
# to create a final reporting table.
#
# Airflow manages scheduling, task dependencies, retries, and monitoring.
# This is similar to traditional ETL pipelines where data is first loaded into
# staging/raw tables and then transformed into final reporting tables.

"""
===============================================================================
S3 TO GCS TO BIGQUERY ETL PIPELINE
===============================================================================

Purpose:
--------
This DAG demonstrates a real-world ETL pipeline using:

1. AWS S3
2. Google Cloud Storage (GCS)
3. BigQuery
4. Apache Airflow

Pipeline Flow:
--------------
AWS S3 Bucket (orders.csv)
            ↓
Apache Airflow Orchestration
            ↓
Copy file from S3 → GCS
            ↓
Load file from GCS → BigQuery Raw Table
            ↓
Run SQL Transformation
            ↓
Create Final Reporting Table

Key Concepts:
-------------
- DAG
- Tasks
- Operators
- Dependencies
- Scheduling
- Retry Handling
- Workflow Orchestration
- Cloud Data Pipeline
"""

# =============================================================================
# 1. IMPORT REQUIRED MODULES
# =============================================================================

# timedelta → used for retry delay timing
from datetime import timedelta

# DAG → used to define Airflow workflow
from airflow import DAG

# days_ago → helper function for DAG start date
from airflow.utils.dates import days_ago

# S3ToGCSOperator → copies files from AWS S3 to Google Cloud Storage
from airflow.providers.amazon.aws.transfers.s3_to_gcs import (
    S3ToGCSOperator
)

# GCSToBigQueryOperator → loads files from GCS into BigQuery tables
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)

# BigQueryInsertJobOperator → runs SQL queries inside BigQuery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)


# =============================================================================
# 2. PROJECT VARIABLES / CONFIGURATION
# =============================================================================
# These variables help avoid hardcoding values repeatedly.
# Makes the DAG reusable and easier to maintain.

# GCP project details
PROJECT_ID = "your-gcp-project-id"
LOCATION = "US"

# -----------------------------------------------------------------------------
# AWS S3 Configuration
# -----------------------------------------------------------------------------

# Source S3 bucket name
AWS_S3_BUCKET = "your-s3-bucket-name"

# File path inside S3 bucket
S3_SOURCE_PATH = "orders/orders.csv"

# -----------------------------------------------------------------------------
# GCS Configuration
# -----------------------------------------------------------------------------

# Target GCS bucket
GCS_BUCKET = "your-gcs-bucket-name"

# Destination path inside GCS
GCS_DESTINATION_PATH = "landing/orders.csv"

# -----------------------------------------------------------------------------
# BigQuery Dataset Names
# -----------------------------------------------------------------------------

# Raw/staging dataset
RAW_DATASET = "raw_ds"

# Final reporting dataset
FINAL_DATASET = "insight_ds"

# -----------------------------------------------------------------------------
# BigQuery Table Names
# -----------------------------------------------------------------------------

# Raw orders table
RAW_TABLE = "orders_raw"

# Final transformed table
FINAL_TABLE = "orders_summary"


# =============================================================================
# 3. DEFAULT DAG ARGUMENTS
# =============================================================================
# Common settings applied to DAG tasks.

ARGS = {

    # DAG owner name
    "owner": "data_engineer",

    # DAG start date
    "start_date": days_ago(1),

    # Current run should not depend on previous run
    "depends_on_past": False,

    # Disable email notifications
    "email_on_failure": False,
    "email_on_retry": False,

    # Retry failed tasks once
    "retries": 1,

    # Wait 5 minutes before retry
    "retry_delay": timedelta(minutes=5),
}


# =============================================================================
# 4. SQL TRANSFORMATION QUERY
# =============================================================================
# This SQL query creates the final reporting table.
#
# Transformation Logic:
# - Reads raw order data
# - Calculates TotalAmount
# - Filters invalid records
# - Creates final reporting table

QUERY = f"""

CREATE OR REPLACE TABLE
`{PROJECT_ID}.{FINAL_DATASET}.{FINAL_TABLE}` AS

SELECT

    -- Order information
    OrderID,
    CustomerID,

    -- Product details
    ProductCategory,

    -- Quantity purchased
    Quantity,

    -- Individual amount
    Amount,

    -- Derived column
    Quantity * Amount AS TotalAmount,

    -- Order date
    OrderDate

FROM
`{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}`

-- Filter invalid records
WHERE OrderID IS NOT NULL

"""


# =============================================================================
# 5. DEFINE THE AIRFLOW DAG
# =============================================================================
# DAG = Directed Acyclic Graph
#
# Represents the complete workflow/pipeline.
#
# schedule_interval = "0 6 * * *"
# → Runs daily at 6 AM

with DAG(

    # Unique DAG name
    dag_id="S3_TO_BIGQUERY_ETL_DAG",

    # DAG schedule
    schedule_interval="0 6 * * *",

    # DAG description
    description="Load data from S3 to BigQuery using GCS staging",

    # Apply default arguments
    default_args=ARGS,

    # Tags visible in Airflow UI
    tags=["airflow", "s3", "gcs", "bigquery", "etl"],

) as dag:

    # =========================================================================
    # 6. TASK 1 - COPY FILE FROM AWS S3 TO GCS
    # =========================================================================
    #
    # Purpose:
    # Copy orders.csv from AWS S3 bucket into GCS bucket.
    #
    # Why?
    # BigQuery integrates natively with GCS, so many companies first
    # stage S3 files into GCS before loading into BigQuery.

    copy_s3_to_gcs = S3ToGCSOperator(

        # Unique task name
        task_id="copy_s3_to_gcs",

        # Source S3 bucket
        bucket=AWS_S3_BUCKET,

        # Source file path
        prefix=S3_SOURCE_PATH,

        # Target GCS path
        dest_gcs=f"gs://{GCS_BUCKET}/landing/",

        # Replace existing file if already present
        replace=True,
    )



    # =========================================================================
    # 7. TASK 2 - LOAD FILE FROM GCS INTO BIGQUERY RAW TABLE
    # =========================================================================
    #
    # Purpose:
    # Load orders.csv from GCS into BigQuery raw table.
    #
    # Target Table:
    # raw_ds.orders_raw

    load_gcs_to_bigquery = GCSToBigQueryOperator(

        # Unique task name
        task_id="load_orders_raw",

        # GCS bucket name
        bucket=GCS_BUCKET,

        # File path inside GCS
        source_objects=[GCS_DESTINATION_PATH],

        # Target BigQuery table
        destination_project_dataset_table=
        f"{RAW_DATASET}.{RAW_TABLE}",

        # Define BigQuery table schema
        schema_fields=[

            {
                "name": "OrderID",
                "type": "INT64",
                "mode": "NULLABLE"
            },

            {
                "name": "CustomerID",
                "type": "INT64",
                "mode": "NULLABLE"
            },

            {
                "name": "ProductCategory",
                "type": "STRING",
                "mode": "NULLABLE"
            },

            {
                "name": "Quantity",
                "type": "INT64",
                "mode": "NULLABLE"
            },

            {
                "name": "Amount",
                "type": "FLOAT64",
                "mode": "NULLABLE"
            },

            {
                "name": "OrderDate",
                "type": "STRING",
                "mode": "NULLABLE"
            },
        ],

        # Overwrite table during every DAG run
        write_disposition="WRITE_TRUNCATE",

        # Source file format
        source_format="CSV",

        # Ignore header row
        skip_leading_rows=1,
    )



    # =========================================================================
    # 8. TASK 3 - RUN BIGQUERY SQL TRANSFORMATION
    # =========================================================================
    #
    # Purpose:
    # Execute SQL transformation query in BigQuery.
    #
    # Output:
    # insight_ds.orders_summary

    transform_orders = BigQueryInsertJobOperator(

        # Unique task name
        task_id="transform_orders",

        # BigQuery job configuration
        configuration={
            "query": {

                # SQL query to execute
                "query": QUERY,

                # Use modern standard SQL
                "useLegacySql": False,
            }
        },

        # BigQuery location
        location=LOCATION,
    )



    # =========================================================================
    # 9. TASK DEPENDENCIES
    # =========================================================================
    #
    # Defines task execution order.
    #
    # Step 1:
    # Copy file from S3 → GCS
    #
    # Step 2:
    # Load file from GCS → BigQuery raw table
    #
    # Step 3:
    # Run SQL transformation
    #
    # Execution Flow:
    #
    # copy_s3_to_gcs
    #           ↓
    # load_gcs_to_bigquery
    #           ↓
    # transform_orders

    copy_s3_to_gcs >> load_gcs_to_bigquery >> transform_orders


# =============================================================================
#  EXPLANATION
# =============================================================================
#
# This DAG demonstrates a common enterprise ETL pattern where:
#
# 1. Data is copied from AWS S3 into Google Cloud Storage
# 2. Airflow orchestrates the pipeline
# 3. Data is loaded into BigQuery raw tables
# 4. SQL transformation creates final reporting tables
#
# Airflow manages:
# - Scheduling
# - Task dependencies
# - Retry handling
# - Monitoring
# - Workflow orchestration
#
# This is similar to traditional ETL pipelines where data is loaded
# into staging tables and transformed into final reporting tables.
#

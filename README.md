# Airflow S3 to BigQuery ETL Pipeline

## Project Overview

This project demonstrates a real-world ETL pipeline using Apache Airflow, AWS S3, Google Cloud Storage (GCS), and BigQuery.

The pipeline copies a CSV file from AWS S3 into GCS, loads it into a BigQuery raw table, and performs a SQL transformation to create a final reporting table.

---

## Pipeline Flow

```text
AWS S3 Bucket
      ↓
Apache Airflow DAG
      ↓
Google Cloud Storage (GCS)
      ↓
BigQuery Raw Table
      ↓
SQL Transformation
      ↓
Final BigQuery Table
```

---

## Repository Structure

```text
airflow-s3-gcs-bigquery-etl/
│
├── dags/
│   └── s3_to_bigquery_etl_dag.py
│
├── data/
│   └── orders.csv
│
├── sql/
│   └── transformation_query.sql
│
├── requirements.txt
└── README.md
```

---

## Technologies Used

- Apache Airflow
- AWS S3
- Google Cloud Storage
- BigQuery
- Python
- SQL

---

## Workflow Steps

1. Copy `orders.csv` from AWS S3 to Google Cloud Storage.
2. Load the file from GCS into a BigQuery raw table called `orders_raw`.
3. Run a BigQuery SQL transformation.
4. Create a final reporting table called `orders_summary`.

---

## Airflow Concepts Covered

- DAG definition
- Tasks and operators
- Task dependencies
- Scheduling
- Retry handling
- Workflow orchestration
- Cloud storage ingestion
- SQL-based transformation

---

## DAG Tasks

| Task ID | Operator | Purpose |
|---|---|---|
| `copy_s3_to_gcs` | `S3ToGCSOperator` | Copies CSV file from AWS S3 to GCS |
| `load_orders_raw` | `GCSToBigQueryOperator` | Loads CSV file from GCS into BigQuery raw table |
| `transform_orders` | `BigQueryInsertJobOperator` | Runs SQL transformation and creates final table |

---

## Interview Explanation

This DAG demonstrates a common enterprise ETL pattern where data is copied from AWS S3 into Google Cloud Storage and then loaded into BigQuery.

Airflow orchestrates the workflow by managing scheduling, dependencies, retries, and monitoring. After the raw data is loaded into BigQuery, a SQL transformation creates the final reporting table.

This is similar to traditional ETL pipelines where data is first loaded into staging or raw tables and then transformed into final reporting tables.

---

## Future Improvements

- Add Airflow sensors to check file availability before processing.
- Add data validation checks before loading into BigQuery.
- Parameterize configs for DEV, QA, and PROD environments.
- Add alerting for failed tasks.
- Add data quality checks after transformation.

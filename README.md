# Airflow S3 to BigQuery ETL Pipeline

## 📌 Project Overview

This project demonstrates a real-world ETL pipeline using Apache Airflow, AWS S3, Google Cloud Storage (GCS), and BigQuery.

The pipeline copies a CSV file from AWS S3 into GCS, loads it into a BigQuery raw table, and performs SQL transformation to create a final reporting table.

---

## 🔄 Pipeline Flow

AWS S3  
→ Airflow DAG  
→ Google Cloud Storage (GCS)  
→ BigQuery Raw Table  
→ SQL Transformation  
→ Final BigQuery Table  

---

## ⚙️ Technologies Used

- Apache Airflow
- AWS S3
- Google Cloud Storage (GCS)
- BigQuery
- Python
- SQL

---

## 📊 Workflow Steps

1. Copy orders.csv from S3 to GCS
2. Load data into BigQuery raw table
3. Run SQL transformation
4. Create final reporting table

---

## 🧠 Key Airflow Concepts

- DAGs
- Tasks
- Operators
- Dependencies
- Scheduling
- Retry Handling
- Workflow Orchestration

---

## 🎯  Explanation

This DAG demonstrates a common enterprise ETL pattern where data is copied from AWS S3 into Google Cloud Storage and then loaded into BigQuery.

Airflow orchestrates the workflow by managing scheduling, dependencies, retries, and monitoring.

After raw data is loaded into BigQuery, a SQL transformation creates the final reporting table.

---

## 🚀 Future Improvements

- Add Airflow Sensors
- Add data quality validation
- Parameterize environment configs
- Add logging and alerting

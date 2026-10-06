from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = "capstone-project-510706"
DATASET_NAME = "raw_ds"
TABLE_NAME = "rides_history"
GCS_BUCKET = "ride_hailing_historical_data"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gcs_to_bq_rides_history",
    default_args=default_args,
    description="Batch load historical CSV ride data from GCS into raw_ds.rides_history",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["capstone", "gcs", "bigquery", "ingestion"],
) as dag:

    load_csv_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_rides_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=["raw/rides_history/*.csv"],  # <--- UPDATED MATCH PATH
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        schema_fields=[
            {"name": "ride_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "rider_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "driver_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pickup_zone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dropoff_zone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pickup_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "dropoff_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "fare", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "surge_multiplier", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "trip_distance", "type": "NUMERIC", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=1,
        source_format="CSV",
        field_delimiter=",",
        allow_quoted_newlines=True,
    )

    load_csv_to_bq
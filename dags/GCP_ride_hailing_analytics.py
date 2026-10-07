from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "capstone-project-510706"
DATASET_NAME = "raw_ds"
STAGING_TABLE = "stg_rides_history"
FINAL_TABLE = "rides_history"
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
    description="Batch load historical CSV ride data from GCS into raw_ds.rides_history with ingest_ts",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["capstone", "gcs", "bigquery", "ingestion"],
) as dag:

    # Step 1: Load raw CSVs into staging table
    load_csv_to_stg = GCSToBigQueryOperator(
        task_id="load_gcs_to_staging",
        bucket=GCS_BUCKET,
        source_objects=["raw/rides_history/*.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{STAGING_TABLE}",
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

    # Step 2: Append CURRENT_TIMESTAMP() as ingest_ts into final table using BigQueryInsertJobOperator
    publish_to_raw_table = BigQueryInsertJobOperator(
        task_id="add_ingest_ts_and_publish",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.{FINAL_TABLE}` AS
                SELECT
                    ride_id,
                    rider_id,
                    driver_id,
                    pickup_zone,
                    dropoff_zone,
                    pickup_ts,
                    dropoff_ts,
                    fare,
                    surge_multiplier,
                    trip_distance,
                    CURRENT_TIMESTAMP() AS ingest_ts
                FROM `{PROJECT_ID}.{DATASET_NAME}.{STAGING_TABLE}`;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task Pipeline Dependency
    load_csv_to_stg >> publish_to_raw_table
import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Modify PROJECT_ID and BUCKET to your GCP values
PROJECT_ID = "de-hdb-analytics-2025"
BUCKET = "de-hdb-analytics-2025-bucket"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PARQUET_FILENAME = "hdb_resale_2017_onwards.parquet"

# Dataset ID for HDB resale data from Jan 2017 onwards
DATASET_ID = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"

# Variables to create table in big query
BIGQUERY_DATASET = "sg_hdb_dataset"
MAIN_TABLE_NAME = "hdb_resale"
TEMP_TABLE_NAME = "hdb_resale_2017_onwards"

# Set default retries for DAG
default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),  # Retry after 30 seconds
}

# Create DAG
hdb_resale_2017_onwards_dag = DAG(
    dag_id="hdb_resale_data_2017_onwards",
    description="Download HDB resale data from data.gov.sg",
    default_args=default_args,
    schedule_interval="0 9 * * *",
    start_date=datetime(2025, 3, 15),
    catchup=False,
    max_active_runs=1,
)


def upload_to_gcs(bucket, object_name, local_file, gcp_conn_id="gcp-airflow"):
    hook = GCSHook(gcp_conn_id)
    hook.upload(
        bucket_name=bucket, object_name=object_name, filename=local_file, timeout=600
    )
    # logging.info(f"File uploaded to gs://{bucket}/{object_name}")


def download_data(**kwargs):
    """Initiate download, poll, and save data from data.gov.sg API"""
    logger = logging.getLogger("airflow.task")

    # Initiate download
    s = requests.Session()
    initiate_download_response = s.get(
        f"https://api-open.data.gov.sg/v1/public/api/datasets/{DATASET_ID}/initiate-download",
        headers={"Content-Type": "application/json"},
        json={},
    )
    response_data = initiate_download_response.json()
    logger.info(response_data["data"]["message"])

    # Poll for download
    MAX_POLLS = 5
    for i in range(MAX_POLLS):
        poll_download_response = s.get(
            f"https://api-open.data.gov.sg/v1/public/api/datasets/{DATASET_ID}/poll-download",
            headers={"Content-Type": "application/json"},
            json={},
        )
        logger.info(f"Poll download response: {poll_download_response.json()}")

        if "url" in poll_download_response.json()["data"]:
            DOWNLOAD_URL = poll_download_response.json()["data"]["url"]
            df = pd.read_csv(DOWNLOAD_URL, dtype=str)

            # Save to a location that Airflow can access
            output_path = f"{AIRFLOW_HOME}/{PARQUET_FILENAME}"
            df.to_parquet(
                output_path,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )

            logger.info(f"Saved {len(df)} rows to {output_path}")
            return {"file_path": output_path, "row_count": len(df)}

        if i == MAX_POLLS - 1:
            logger.error(
                f"{i + 1}/{MAX_POLLS}: No result found, possible error with dataset, please try again or inform source provider at https://go.gov.sg/datagov-supportform\n"
            )
            raise Exception("Failed to download data after maximum poll attempts")
        else:
            logger.warning(f"{i + 1}/{MAX_POLLS}: No result yet, continuing to poll\n")
        time.sleep(5)


with hdb_resale_2017_onwards_dag:
    # Task 1: Download csv data and save as parquet
    download_task = PythonOperator(
        task_id="download_hdb_resale_data",
        python_callable=download_data,
        provide_context=True,
    )

    # Task 2: Upload file to google storage
    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"hdb_resale/{PARQUET_FILENAME}",
            "local_file": f"{AIRFLOW_HOME}/{PARQUET_FILENAME}",
            "gcp_conn_id": "gcp-airflow",
        },
    )

    # Task 3: Create hdb_resale main table
    create_main_table_task = BigQueryInsertJobOperator(
        task_id="create_main_table",
        gcp_conn_id="gcp-airflow",
        configuration={
            "query": {
                "query": f"""
                    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BIGQUERY_DATASET}.{MAIN_TABLE_NAME}` (
                        unique_id STRING,
                        row_md5sum STRING,
                        row_counter INTEGER,
                        month STRING,
                        town STRING,
                        flat_type STRING,
                        block STRING,
                        street_name STRING,
                        storey_range STRING,
                        floor_area_sqm FLOAT64,
                        flat_model STRING,
                        lease_commence_date INTEGER,
                        remaining_lease STRING,
                        resale_price NUMERIC
                    )
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task 4: Create external table
    create_external_table_task = BigQueryInsertJobOperator(
        task_id="create_external_table",
        gcp_conn_id="gcp-airflow",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{TEMP_TABLE_NAME}_ext`
                    OPTIONS (
                        uris = ['gs://{BUCKET}/hdb_resale/{PARQUET_FILENAME}'],
                        format = 'PARQUET'
                    );
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task 5: Create staging table
    create_staging_table_task = BigQueryInsertJobOperator(
        task_id="create_staging_table",
        gcp_conn_id="gcp-airflow",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{TEMP_TABLE_NAME}_stg` AS
                    WITH base_data AS (
                        SELECT
                            -- Use TO_HEX to convert MD5 bytes to string type
                            TO_HEX(MD5(CONCAT(
                                COALESCE(month, ""), '|',
                                COALESCE(town, ""), '|',
                                COALESCE(flat_type, ""), '|',
                                COALESCE(block, ""), '|',
                                COALESCE(street_name, ""), '|',
                                COALESCE(storey_range, ""), '|',
                                COALESCE(floor_area_sqm, ""), '|',
                                COALESCE(flat_model, ""), '|',
                                COALESCE(lease_commence_date, ""), '|',
                                COALESCE(remaining_lease, ""), '|',
                                COALESCE(resale_price, "")
                            ))) AS row_md5sum,
                            month,
                            town,
                            flat_type,
                            block,
                            street_name,
                            storey_range,
                            SAFE_CAST(floor_area_sqm AS FLOAT64) AS floor_area_sqm,
                            flat_model,
                            SAFE_CAST(lease_commence_date AS INTEGER) AS lease_commence_date,
                            remaining_lease,
                            SAFE_CAST(resale_price AS NUMERIC) AS resale_price,
                        FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{TEMP_TABLE_NAME}_ext`
                    ),
                    unique_rows AS (
                        SELECT
                            *,
                            ROW_NUMBER() OVER(
                                PARTITION BY row_md5sum 
                                ORDER BY month, town, block, street_name
                            ) AS row_counter,
                        FROM base_data
                    )
                    SELECT
                        -- Create a unique identifier by concatenating hashsum and counter
                        CONCAT(row_md5sum, '-', 
                            ROW_NUMBER() OVER(PARTITION BY row_md5sum ORDER BY month, town, block, street_name)
                        ) AS unique_id,
                        row_md5sum,
                        row_counter,
                        month,
                        town,
                        flat_type,
                        block,
                        street_name,
                        storey_range,
                        floor_area_sqm,
                        flat_model,
                        lease_commence_date,
                        remaining_lease,
                        resale_price
                    FROM unique_rows;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task 6: Incremental load to main table
    merge_to_main_table_task = BigQueryInsertJobOperator(
        task_id="merge_to_main_table",
        gcp_conn_id="gcp-airflow",
        configuration={
            "query": {
                "query": f"""
                    -- Merge using unique_id as the matching key
                    -- This will only insert new records, preserving all existing data
                    MERGE INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.{MAIN_TABLE_NAME}` T
                    USING `{PROJECT_ID}.{BIGQUERY_DATASET}.{TEMP_TABLE_NAME}_stg` S
                    ON T.unique_id = S.unique_id
                    
                    -- Only when not matched, insert the new row
                    WHEN NOT MATCHED THEN
                        INSERT (
                            unique_id,
                            row_md5sum,
                            row_counter,
                            month,
                            town,
                            flat_type,
                            block,
                            street_name,
                            storey_range,
                            floor_area_sqm,
                            flat_model,
                            lease_commence_date,
                            remaining_lease,
                            resale_price
                        )
                        VALUES (
                            S.unique_id,
                            S.row_md5sum,
                            S.row_counter,
                            S.month,
                            S.town,
                            S.flat_type,
                            S.block,
                            S.street_name,
                            S.storey_range,
                            S.floor_area_sqm,
                            S.flat_model,
                            S.lease_commence_date,
                            S.remaining_lease,
                            S.resale_price
                        );
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task 7: clean up downloaded file
    cleanup_task = BashOperator(
        task_id="cleanup_files",
        bash_command=f"rm {AIRFLOW_HOME}/{PARQUET_FILENAME}",
    )

(
    download_task
    >> upload_to_gcs_task
    >> create_main_table_task
    >> create_external_table_task
    >> create_staging_table_task
    >> merge_to_main_table_task
    >> cleanup_task
)

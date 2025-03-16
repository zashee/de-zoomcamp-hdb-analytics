import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Modify PROJECT_ID and BUCKET to your GCP values
PROJECT_ID = "de-hdb-analytics-2025"
BUCKET = "de-hdb-analytics-2025-bucket"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PARQUET_FILENAME = "hdb_resale_2017_onwards.parquet"

# Dataset ID for HDB resale data from Jan 2017 onwards
DATASET_ID = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"


# Create DAG
hdb_resale_2017_onwards_dag = DAG(
    dag_id="hdb_resale_data_2017_onwards",
    description="Download HDB resale data from data.gov.sg",
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

            # Add sequential _id column
            df = df.reset_index(drop=True).rename_axis("_id").reset_index()
            df["_id"] += 1  # Start IDs at 1 instead of 0
            df["_id"] = df["_id"].astype("int32")  # Convert _id to int32

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
        time.sleep(3)


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
        retries=10,
    )

    cleanup_task = BashOperator(
        task_id="cleanup_files",
        bash_command=f"rm {AIRFLOW_HOME}/{PARQUET_FILENAME}",
    )

download_task >> upload_to_gcs_task >> cleanup_task

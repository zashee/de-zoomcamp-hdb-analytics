# Data Engineering Zoomcamp HDB Analytics Project

# Reproduce Project
## (Optional) Setup uv package manager.
1. uv python install 3.12 (install python version)
2. uv init (init project)
3. uv venv --python 3.12 (install venv)


## 1. Setup GCP account
1. In Google Cloud Navigation menu go to IAM & Admin -> Service Accounts page. Click "+ CREATE SERVICE ACCOUNT" button to setup your service account for terraform and airflow.
2. Add roles to your service account:
    * Storage Admin (for managing cloud storage bucket)
    * BigQuery Admin (for managing BigQuery datasets and tables)
3. Enable IAM APIs
    * https://console.cloud.google.com/apis/library/iam.googleapis.com
    * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
4. In Service Accounts page, find your service account and click Actions menu -> KEYS tab -> click "ADD KEY" button to create your service account JSON key.
5. Save the JSON key to local machine.

## 2. Terraform
1. Download terraform
2. Edit variables.tf to point to your desires locations.
3. Export GOOGLE_APPLICATION_CREDENTIALS=<path to credentials>
4. terraform init
5. terraform plan
6. terraform apply

## 3. Airflow
1. Create logs folder in `infrastructure/airflow/` directory and give permissions to airflow user (UID 50000) to write to logs directory.
run below commands from airflow directory
```bash
mkdir logs
sudo chown -R 50000:0 logs
```
2. Build the docker images defined in docker-compose.yaml with `docker compose build`.
3. Run the containers with `docker compose up -d`.
4. Create gcp connection for airflow:
    1. In the top menu, click on Admin tab -> select Connections in the dropdown menu.
    2. On the Connections page click the "+" button to add a new record.
    3. Fill in the following details
        * Connection Id: gcp-airflow
        * Connection Type: Google Cloud
        * ProjectId: your google cloud project id
        * Keyfile Path: /opt/airflow/gcp/<json credentials filename>
    4. Click save at the bottom of the page.
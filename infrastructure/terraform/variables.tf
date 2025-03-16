variable "credentials_file" {
  description = "Path to the GCP service account credentials JSON file"
  type        = string
  default     = "" # Empty default, use GOOGLE_APPLICATION_CREDENTIALS env var instead
}

variable "project" {
  description = "Project"
  default     = "de-hdb-analytics-2025"
}

variable "region" {
  description = "Project Region"
  type        = string
  # Update the below to your desired region
  default = "asia-southeast1"
}

variable "gcs_location" {
  description = "GCS Bucket Location"
  type        = string
  # Update the below to your desired location
  default = "asia-southeast1"
}

variable "bq_location" {
  description = "BigQuery Location"
  type        = string
  default     = "asia-southeast1"
}

variable "bq_dataset_name" {
  description = "SG HDB Dataset"
  type        = string
  # Update the below to what you want your dataset to be called
  default = "sg_hdb_dataset"
}

variable "gcs_bucket_name" {
  description = "SG HDB Data Lake"
  type        = string
  # Update the below to a unique bucket name
  default = "de-hdb-analytics-2025-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  type        = string
  default     = "STANDARD"
}

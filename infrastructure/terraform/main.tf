terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = var.credentials_file != "" ? file(var.credentials_file) : null
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "de-zoomcamp-bucket" {
  name          = var.gcs_bucket_name
  location      = var.gcs_location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "de-zoomcamp_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.bq_location
}

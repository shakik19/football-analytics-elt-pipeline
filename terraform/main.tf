terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.23.0"
    }
  }
}

provider "google" {
  project     = var.project
  credentials = file(var.credentials)
  region      = var.region
}

resource "google_storage_bucket" "gcs_bucket" {
  name     = var.gcs_bucket_name
  location = var.region

  public_access_prevention = "enforced"

  force_destroy = true
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = var.bq_dataset_id
  location                   = var.region
  delete_contents_on_destroy = true
}

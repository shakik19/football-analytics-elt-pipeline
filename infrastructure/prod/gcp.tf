provider "google" {
  project     = var.PROJECT_ID
  credentials = file(var.GCP_JSON_KEY_FILEPATH)
  region      = var.REGION
}

resource "google_storage_bucket" "gcs_bucket" {
  name     = "transfermarkt-data"
  location = var.REGION

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

resource "google_bigquery_dataset" "dataset_seed" {
  dataset_id                 = "transfermarkt_seed"
  location                   = var.REGION
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "dataset_core" {
  dataset_id                 = "transfermarkt_core"
  location                   = var.REGION
  delete_contents_on_destroy = true
}

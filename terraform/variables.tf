variable "credentials" {
  description = "Your project's gcp credentials file path(absolute path recommended)"
  default = "../credentials/gcp_key.json"
}

variable "project" {
  description = "Project id"
  default = "fast-forward-412713"
}

variable "region" {
  description = <<EOT
  Region that will be used to create cloud storage bucket and bigquery dataset
  e.g. us-west1
  EOT

  default = "us-west1"
}

variable "location" {
  description = "Project location"
  default = "US"
}

variable "bq_seed_dataset_id" {
  description = <<EOT
  A unique dataset id. Please check the official documentation for details
  naming convention
  EOT

  default = "transfermarkt_seed"
}

variable "bq_core_dataset_id" {
  description = <<EOT
  A unique dataset id. Please check the official documentation for details
  naming convention
  EOT

  default = "transfermarkt_core"
}

variable "gcs_bucket_name" {
  description = <<EOT
  A globally unique bucket name. Please check the official documentation for
  detail naming convention
  EOT

  default = "transfermarkt-data"
}

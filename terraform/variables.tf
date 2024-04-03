variable "credentials" {
  description = "Your project's gcp credentials file path(absolute path recommended)"
  default = ""
}

variable "project" {
  description = "Project id"
  default = ""
}

variable "region" {
  description = <<EOT
  Region that will be used to create cloud storage bucket and bigquery dataset
  e.g. us-west1
  EOT

  default = ""
}

variable "location" {
  description = "Project location"
  default = ""
}

variable "bq_dataset_id" {
  description = <<EOT
  A unique dataset id. Please check the official documentation for details
  naming convention
  EOT

  default = ""
}

variable "gcs_bucket_name" {
  description = <<EOT
  A globally unique bucket name. Please check the official documentation for
  detail naming convention
  EOT

  default = ""
}

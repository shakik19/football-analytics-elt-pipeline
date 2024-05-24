variable "service_account" {
  description = "Your project's gcp credentials file path(absolute path recommended)"
  type = string
  sensitive = true
}

variable "project_id" {
  description = "Project id"
  default = "data-pipeline"
  type = string
}

variable "region" {
  description = <<EOT
  Region that will be used to create cloud storage bucket and bigquery dataset
  e.g. us-west1
  EOT
  type = string
  default = "us-west1"
}

variable "location" {
  description = "Project location"
  default = "US"
  type = string
}

variable "seed_dataset_id" {
  description = <<EOT
  A unique dataset id. Please check the official documentation for details
  naming convention
  EOT

  default = "transfermarkt_seed"
  type = string
}

variable "core_dataset_id" {
  description = <<EOT
  A unique dataset id. Please check the official documentation for details
  naming convention
  EOT

  default = "transfermarkt_core"
  type = string
}

variable "bucket_name" {
  description = <<EOT
  A globally unique bucket name. Please check the official documentation for
  detail naming convention
  EOT

  default = "transfermarkt-data"
  type = string
}

variable "service_account" {
  description = "Your project's gcp credentials file path(absolute path recommended)"
  type = string
  sensitive = true
}

variable "project_id" {
  description = "Project id"
  type = string
}

variable "region" {
  description = <<EOT
  Region that will be used to create cloud storage bucket and bigquery dataset
  e.g. us-west1
  EOT
  type = string
}

variable "location" {
  description = "Project location"
  type = string
}

variable "seed_dataset_id" {
  description = <<EOT
  A unique dataset id. Please check the official documentation for details
  naming convention
  EOT

  type = string
}

variable "core_dataset_id" {
  description = <<EOT
  A unique dataset id. Please check the official documentation for details
  naming convention
  EOT

  type = string
}

variable "bucket_name" {
  description = <<EOT
  A globally unique bucket name. Please check the official documentation for
  detail naming convention
  EOT

  type = string
}

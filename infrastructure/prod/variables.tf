#? OPTIONAL configurable variables
variable "REGION" {
  description = <<EOT
  Region that will be used to create Astro Deployment, cloud storage bucket and bigquery datasets
  Regions can only be one of us-central1, us-east4 and europe-west4  
  EOT
  default = "us-central1"
  type = string
}

variable "DEPLOYMENT_NAME" {
  description = "The name of the deployment"
  default = "transfermarkt-prod"
  type = string
}

variable "CONTACT_EMAILS" {
  description = "Emails to send alerts and updates"
  default = []
  type = list(string)
}


#? The variables below are set through Environment Variables. Configuration isn't required
variable "GCP_JSON_KEY_FILEPATH" {
  description = "Your project's gcp credentials file path(absolute path recommended)"
  type = string
  sensitive = true
}

variable "PROJECT_ID" {
  description = "GCP Project id under which GCP resources should be initialized"
  type = string
}

variable "ASTRO_API_TOKEN" {
  description = "An organization scoped api token of Astro Cloud with OWNER permission"
  type      = string
  sensitive = true
}

variable "ASTRO_ORGANIZATION_ID" {
  description = "You astro cloud organization id (Required)"
  type      = string
  sensitive = true
}

variable "ASTRO_WORKSPACE_ID" {
  description = "An existing workspace id if you are not creating a fresh workspace"
  type      = string
  sensitive = true
}

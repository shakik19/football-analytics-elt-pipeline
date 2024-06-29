provider "astro" {
  organization_id = var.ASTRO_ORGANIZATION_ID
  token           = var.ASTRO_API_TOKEN
}

resource "astro_deployment" "standard" {
  workspace_id                   = var.ASTRO_WORKSPACE_ID
  original_astro_runtime_version = "11.6.0"
  name                           = "transfermarkt-${var.ENV_NAME}"
  description                    = "A deployment for the transfermarkt data pipeline"
  type                           = "STANDARD"
  cloud_provider                 = "GCP"
  region                         = "us-central1"
  contact_emails                 = var.CONTACT_EMAILS
  default_task_pod_cpu           = "0.25"
  default_task_pod_memory        = "0.5Gi"
  executor                       = "CELERY"
  is_cicd_enforced               = true
  is_dag_deploy_enabled          = true
  is_development_mode            = true
  is_high_availability           = false
  resource_quota_cpu             = "4"
  resource_quota_memory          = "8Gi"
  scheduler_size                 = "SMALL"
  # The config below makes the deployment stale during the specified time. To use it make the is_development_mode=false 
  scaling_spec = {
    hibernation_spec = {
      schedules = [{
        is_enabled        = true
        wake_at_cron      = "10 6 * * 3"
        hibernate_at_cron = "30 6 * * 3"
      }]
    }
  }
  worker_queues = [
    {
      name               = "default"
      is_default         = true
      astro_machine      = "A10"
      max_worker_count   = 1
      min_worker_count   = 1
      worker_concurrency = 16
  }]
  environment_variables = [
    {
      key       = "GCP_SA_PRIVATE_KEY_ID"
      value     = var.GCP_SA_PRIVATE_KEY_ID
      is_secret = true
    },
    {
      key       = "GCP_SA_PRIVATE_KEY"
      value     = var.GCP_SA_PRIVATE_KEY
      is_secret = true
    },
    {
      key       = "GCP_SA_CLIENT_EMAIL"
      value     = var.GCP_SA_CLIENT_EMAIL
      is_secret = true
    },
    {
      key       = "GCP_SA_CLIENT_ID"
      value     = var.GCP_SA_CLIENT_ID
      is_secret = true
    },
    {
      key       = "CLIENT_X509_CERT_URL"
      value     = var.CLIENT_X509_CERT_URL
      is_secret = true
    },
    {
      key       = "KAGGLE_USERNAME"
      value     = var.KAGGLE_USERNAME
      is_secret = true
    },
    {
      key       = "KAGGLE_KEY"
      value     = var.KAGGLE_KEY
      is_secret = true
    },
    {
      key       = "ENV_NAME"
      value     = var.ENV_NAME
      is_secret = false
    },
    {
      key       = "PROJECT_ID"
      value     = var.PROJECT_ID
      is_secret = false
    },
    {
      key       = "REGION"
      value     = var.REGION
      is_secret = false
    },
    {
      key       = "SEED_DATASET_NAME",
      value     = "${var.ENV_NAME}_transfermarkt_seed"
      is_secret = false
    },
    {
      key       = "CORE_DATASET_NAME",
      value     = "${var.ENV_NAME}_transfermarkt_core"
      is_secret = false
    },
    {
      key       = "BUCKET_NAME"
      value     = "${var.ENV_NAME}-transfermarkt-data"
      is_secret = false
  }]
}

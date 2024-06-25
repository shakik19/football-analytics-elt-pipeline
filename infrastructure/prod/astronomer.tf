provider "astro" {
  organization_id = var.ASTRO_ORGANIZATION_ID
  token           = var.ASTRO_API_TOKEN
}

resource "astro_deployment" "standard" {
  workspace_id                   = var.ASTRO_WORKSPACE_ID
  original_astro_runtime_version = "11.5.0"
  name                           = var.DEPLOYMENT_NAME
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
  resource_quota_cpu             = "8"
  resource_quota_memory          = "16Gi"
  scheduler_size                 = "SMALL"
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
      astro_machine      = "A5"
      max_worker_count   = 2
      min_worker_count   = 0
      worker_concurrency = 1
    },
    {
      name               = "heavy-tasks"
      is_default         = false
      astro_machine      = "A20"
      max_worker_count   = 1
      min_worker_count   = 0
      worker_concurrency = 8
  }]
  environment_variables = [
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
      value     = "transfermarkt_seed"
      is_secret = false
    },
    {
      key       = "CORE_DATASET_NAME",
      value     = "transfermarkt_core"
      is_secret = false
    },
    {
      key       = "BUCKET_NAME"
      value     = "transfermarkt-data"
      is_secret = false
    },
    {
      key       = "DBT_PROFILES_TARGET"
      value     = "prod"
      is_secret = false
  }]
}

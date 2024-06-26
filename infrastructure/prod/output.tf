output "worker_queues" {
  value = astro_deployment.standard.worker_queues
}

output "scheduler_status" {
  value = astro_deployment.standard.status
}

output "scheduler_status_reason" {
  value = astro_deployment.standard.status_reason
}

output "scheduler_size" {
  value = astro_deployment.standard.scheduler_size
}

output "scheduler_cpu" {
  value = astro_deployment.standard.scheduler_cpu
}

output "scheduler_memory" {
  value = astro_deployment.standard.scheduler_memory
}

output "resource_quota_cpu" {
  value = astro_deployment.standard.resource_quota_cpu
}

output "resource_quota_memory" {
  value = astro_deployment.standard.resource_quota_memory
}

output "scaling_spec" {
  value = astro_deployment.standard.scaling_spec
}

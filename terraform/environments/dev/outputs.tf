output "api_endpoint" {
  description = "DNS name of the internal load balancer fronting the API."
  value       = module.application.load_balancer_dns_name
}

output "cluster_name" {
  description = "ECS cluster name."
  value       = module.application.cluster_name
}

output "migration_task_definition_arn" {
  description = "Task definition to run before rolling a deployment."
  value       = module.application.migration_task_definition_arn
}

output "expected_kafka_topics" {
  description = "Topics that must exist before the outbox publisher runs."
  value       = module.messaging.expected_topics
}

output "database_endpoint" {
  description = "RDS endpoint."
  value       = module.database.endpoint
}

output "cache_endpoint" {
  description = "ElastiCache primary endpoint."
  value       = module.cache.primary_endpoint
}

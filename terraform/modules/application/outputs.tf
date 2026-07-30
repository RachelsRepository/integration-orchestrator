output "cluster_name" {
  description = "Name of the ECS cluster."
  value       = aws_ecs_cluster.this.name
}

output "load_balancer_dns_name" {
  description = "DNS name of the load balancer."
  value       = aws_lb.this.dns_name
}

output "load_balancer_zone_id" {
  description = "Hosted zone id of the load balancer, for an alias record."
  value       = aws_lb.this.zone_id
}

output "alb_security_group_id" {
  description = "Security group attached to the load balancer."
  value       = aws_security_group.alb.id
}

output "api_service_name" {
  description = "Name of the API service."
  value       = aws_ecs_service.api.name
}

output "workers_service_name" {
  description = "Name of the worker service."
  value       = aws_ecs_service.workers.name
}

output "migration_task_definition_arn" {
  description = "Task definition the deployment pipeline runs before rolling the services."
  value       = aws_ecs_task_definition.migrate.arn
}

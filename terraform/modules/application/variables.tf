variable "name" {
  description = "Name prefix applied to every resource in this module."
  type        = string
}

variable "environment" {
  description = "Value supplied to the application's ENVIRONMENT setting."
  type        = string

  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "environment must be one of development, staging or production."
  }
}

variable "region" {
  description = "AWS region, used for the log driver configuration."
  type        = string
}

variable "vpc_id" {
  description = "VPC the service runs in."
  type        = string
}

variable "public_subnet_ids" {
  description = "Subnets for the load balancer."
  type        = list(string)
}

variable "private_subnet_ids" {
  description = "Subnets for the ECS tasks."
  type        = list(string)
}

variable "task_security_group_id" {
  description = "Security group the ECS tasks run in. Owned by the caller so the data tier modules can reference it without forming a dependency cycle."
  type        = string
}

variable "image" {
  description = "Fully qualified container image reference, including the tag or digest."
  type        = string
}

variable "container_port" {
  description = "Port the API listens on."
  type        = number
  default     = 8000
}

variable "certificate_arn" {
  description = "ACM certificate for the HTTPS listener."
  type        = string
}

variable "ssl_policy" {
  description = "TLS policy for the listener."
  type        = string
  default     = "ELBSecurityPolicy-TLS13-1-2-2021-06"
}

variable "ingress_cidr_blocks" {
  description = "Networks permitted to reach the load balancer. This is an internal API."
  type        = list(string)
}

variable "internal_load_balancer" {
  description = "Place the load balancer on private subnets only."
  type        = bool
  default     = true
}

variable "alb_idle_timeout_seconds" {
  description = "Load balancer idle timeout. Must exceed the longest provider timeout budget."
  type        = number
  default     = 65
}

variable "database_secret_arn" {
  description = "Secret containing the database connection URL."
  type        = string
}

variable "jwt_secret_arn" {
  description = "Secret containing the token verification key."
  type        = string
}

variable "provider_secret_arns" {
  description = "Map of provider slug to the ARN of its credentials secret."
  type        = map(string)
  default     = {}
}

variable "secrets_read_policy_arn" {
  description = "Policy granting the execution role read access to those secrets."
  type        = string
}

variable "redis_url" {
  description = "Redis connection URL."
  type        = string
}

variable "kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers."
  type        = string
}

variable "kafka_resource_arns" {
  description = "MSK cluster, topic and group ARNs the task role may act on."
  type        = list(string)
}

variable "extra_environment" {
  description = "Additional plain environment variables."
  type = list(object({
    name  = string
    value = string
  }))
  default = []
}

variable "log_level" {
  description = "Application log level."
  type        = string
  default     = "INFO"
}

variable "log_retention_days" {
  description = "Retention for the task log groups."
  type        = number
  default     = 30
}

variable "tracing_enabled" {
  description = "Export OpenTelemetry traces."
  type        = bool
  default     = true
}

variable "otlp_endpoint" {
  description = "OTLP collector endpoint."
  type        = string
  default     = "http://localhost:4317"
}

variable "cpu_architecture" {
  description = "Task CPU architecture."
  type        = string
  default     = "ARM64"
}

variable "api_cpu" {
  description = "CPU units for an API task."
  type        = string
  default     = "512"
}

variable "api_memory" {
  description = "Memory in MiB for an API task."
  type        = string
  default     = "1024"
}

variable "api_desired_count" {
  description = "Initial API task count."
  type        = number
  default     = 2
}

variable "api_min_capacity" {
  description = "Minimum API task count."
  type        = number
  default     = 2
}

variable "api_max_capacity" {
  description = "Maximum API task count."
  type        = number
  default     = 10
}

variable "api_target_cpu_utilization" {
  description = "Target average CPU utilisation for the API service."
  type        = number
  default     = 60
}

variable "worker_cpu" {
  description = "CPU units for a worker task."
  type        = string
  default     = "512"
}

variable "worker_memory" {
  description = "Memory in MiB for a worker task."
  type        = string
  default     = "1024"
}

variable "worker_desired_count" {
  description = "Initial worker task count."
  type        = number
  default     = 2
}

variable "worker_min_capacity" {
  description = "Minimum worker task count."
  type        = number
  default     = 1
}

variable "worker_max_capacity" {
  description = "Maximum worker task count."
  type        = number
  default     = 6
}

variable "worker_target_cpu_utilization" {
  description = "Target average CPU utilisation for the worker service."
  type        = number
  default     = 70
}

variable "worker_stop_timeout_seconds" {
  description = "Grace period a worker task gets to finish its current batch."
  type        = number
  default     = 60

  validation {
    condition     = var.worker_stop_timeout_seconds <= 120
    error_message = "Fargate caps stopTimeout at 120 seconds."
  }
}

variable "container_insights_enabled" {
  description = "Enable Container Insights on the cluster."
  type        = bool
  default     = true
}

variable "enable_execute_command" {
  description = "Allow ECS Exec into running tasks. Useful in development, audited in production."
  type        = bool
  default     = false
}

variable "deletion_protection" {
  description = "Refuse to delete the load balancer through the API."
  type        = bool
  default     = true
}

variable "alarm_5xx_threshold" {
  description = "Server errors per minute before the alarm fires."
  type        = number
  default     = 5
}

variable "alarm_topic_arns" {
  description = "SNS topics notified when an alarm changes state."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}

variable "project" {
  description = "Project slug used as a name prefix."
  type        = string
  default     = "integration-orchestrator"
}

variable "environment" {
  description = "Environment name used in resource names and tags."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr_block" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "availability_zones" {
  description = "Availability zones to spread subnets across."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "image" {
  description = "Container image reference, including tag or digest."
  type        = string
}

variable "certificate_arn" {
  description = "ACM certificate for the HTTPS listener."
  type        = string
}

variable "ingress_cidr_blocks" {
  description = "Networks permitted to reach the internal load balancer."
  type        = list(string)
  default     = ["10.20.0.0/16"]
}

variable "provider_slugs" {
  description = "Integration providers whose credentials get a secret."
  type        = list(string)
  default     = ["northstar", "meridian", "cobalt"]
}

variable "database_instance_class" {
  description = "RDS instance class."
  type        = string
  default     = "db.t4g.medium"
}

variable "cache_node_type" {
  description = "ElastiCache node type."
  type        = string
  default     = "cache.t4g.small"
}

variable "kafka_broker_instance_type" {
  description = "MSK broker instance type."
  type        = string
  default     = "kafka.t3.small"
}

variable "otlp_endpoint" {
  description = "OTLP collector endpoint reachable from the tasks."
  type        = string
  default     = "http://localhost:4317"
}

variable "alarm_topic_arns" {
  description = "SNS topics notified when an alarm changes state."
  type        = list(string)
  default     = []
}

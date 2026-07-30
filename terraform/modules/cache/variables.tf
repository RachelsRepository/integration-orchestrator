variable "name" {
  description = "Name prefix applied to every resource in this module."
  type        = string
}

variable "vpc_id" {
  description = "VPC the cache lives in."
  type        = string
}

variable "subnet_ids" {
  description = "Data subnets for the cache subnet group."
  type        = list(string)
}

variable "allowed_security_group_ids" {
  description = "Security groups permitted to reach Redis."
  type        = list(string)
  default     = []
}

variable "engine_version" {
  description = "Redis engine version."
  type        = string
  default     = "7.1"
}

variable "parameter_group_family" {
  description = "Parameter group family matching the engine version."
  type        = string
  default     = "redis7"
}

variable "node_type" {
  description = "Cache node type."
  type        = string
  default     = "cache.t4g.small"
}

variable "replica_count" {
  description = "Number of read replicas. Any value above zero enables automatic failover."
  type        = number
  default     = 1
}

variable "transit_encryption_enabled" {
  description = "Require TLS for client connections."
  type        = bool
  default     = true
}

variable "maintenance_window" {
  description = "Weekly maintenance window in UTC."
  type        = string
  default     = "sun:05:30-sun:06:30"
}

variable "apply_immediately" {
  description = "Apply modifications immediately rather than in the maintenance window."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}

variable "name" {
  description = "Cluster name and resource prefix."
  type        = string
}

variable "vpc_id" {
  description = "VPC the cluster lives in."
  type        = string
}

variable "subnet_ids" {
  description = "Data subnets for the broker nodes."
  type        = list(string)
}

variable "allowed_security_group_ids" {
  description = "Security groups permitted to reach the brokers."
  type        = list(string)
  default     = []
}

variable "kafka_version" {
  description = "Kafka version."
  type        = string
  default     = "3.6.0"
}

variable "broker_count" {
  description = "Number of broker nodes. Must be a multiple of the subnet count."
  type        = number
  default     = 3
}

variable "broker_instance_type" {
  description = "Broker instance type."
  type        = string
  default     = "kafka.m7g.large"
}

variable "broker_storage_gb" {
  description = "EBS volume size per broker."
  type        = number
  default     = 100
}

variable "default_partitions" {
  description = "Default partition count for new topics."
  type        = number
  default     = 6
}

variable "default_replication_factor" {
  description = "Default replication factor for new topics."
  type        = number
  default     = 3
}

variable "min_insync_replicas" {
  description = "Replicas that must acknowledge a write when producers use acks=all."
  type        = number
  default     = 2

  validation {
    condition     = var.min_insync_replicas >= 2
    error_message = "At least two in-sync replicas are required to survive a single broker failure without data loss."
  }
}

variable "retention_hours" {
  description = "Default topic retention."
  type        = number
  default     = 168
}

variable "log_retention_days" {
  description = "Retention for broker logs in CloudWatch."
  type        = number
  default     = 30
}

variable "kms_key_arn" {
  description = "Customer-managed KMS key for encryption at rest."
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}

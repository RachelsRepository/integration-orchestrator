variable "name" {
  description = "Name prefix applied to every resource in this module."
  type        = string
}

variable "vpc_id" {
  description = "VPC the database lives in."
  type        = string
}

variable "subnet_ids" {
  description = "Data subnets for the DB subnet group."
  type        = list(string)
}

variable "allowed_security_group_ids" {
  description = "Security groups permitted to reach PostgreSQL."
  type        = list(string)
  default     = []
}

variable "engine_version" {
  description = "PostgreSQL engine version."
  type        = string
  default     = "16.4"
}

variable "parameter_group_family" {
  description = "Parameter group family matching the engine version."
  type        = string
  default     = "postgres16"
}

variable "instance_class" {
  description = "RDS instance class."
  type        = string
  default     = "db.t4g.medium"
}

variable "allocated_storage_gb" {
  description = "Initial storage allocation."
  type        = number
  default     = 50
}

variable "max_allocated_storage_gb" {
  description = "Upper bound for storage autoscaling."
  type        = number
  default     = 200
}

variable "database_name" {
  description = "Name of the application database."
  type        = string
  default     = "orchestrator"
}

variable "master_username" {
  description = "Master username. The password is generated and stored in Secrets Manager."
  type        = string
  default     = "orchestrator"
}

variable "multi_az" {
  description = "Run a standby in a second availability zone."
  type        = bool
  default     = true
}

variable "backup_retention_days" {
  description = "How many days of automated backups to keep."
  type        = number
  default     = 14

  validation {
    condition     = var.backup_retention_days >= 1
    error_message = "Automated backups must be enabled; set at least one day of retention."
  }
}

variable "backup_window" {
  description = "Daily backup window in UTC."
  type        = string
  default     = "03:00-04:00"
}

variable "maintenance_window" {
  description = "Weekly maintenance window in UTC."
  type        = string
  default     = "sun:04:30-sun:05:30"
}

variable "statement_timeout_ms" {
  description = "Server-side statement timeout."
  type        = number
  default     = 15000
}

variable "idle_transaction_timeout_ms" {
  description = "How long a session may hold an idle transaction before it is terminated."
  type        = number
  default     = 30000
}

variable "log_min_duration_ms" {
  description = "Log statements slower than this."
  type        = number
  default     = 500
}

variable "performance_insights_enabled" {
  description = "Enable Performance Insights."
  type        = bool
  default     = true
}

variable "enhanced_monitoring_interval" {
  description = "Enhanced monitoring interval in seconds. Zero disables it."
  type        = number
  default     = 60
}

variable "deletion_protection" {
  description = "Refuse to delete the instance through the API."
  type        = bool
  default     = true
}

variable "skip_final_snapshot" {
  description = "Skip the final snapshot on destroy. Only ever sensible for throwaway estates."
  type        = bool
  default     = false
}

variable "apply_immediately" {
  description = "Apply modifications immediately rather than in the maintenance window."
  type        = bool
  default     = false
}

variable "kms_key_arn" {
  description = "Customer-managed KMS key for storage encryption. Uses the AWS-managed key when null."
  type        = string
  default     = null
}

variable "secret_recovery_window_days" {
  description = "Recovery window for the credentials secret."
  type        = number
  default     = 7
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}

variable "name" {
  description = "Name prefix applied to every secret."
  type        = string
}

variable "provider_slugs" {
  description = "Provider slugs that need a credentials secret."
  type        = list(string)
  default     = ["northstar", "meridian", "cobalt"]
}

variable "additional_secret_arns" {
  description = "Secrets created elsewhere that the tasks must also read, such as the database URL."
  type        = list(string)
  default     = []
}

variable "recovery_window_days" {
  description = "Recovery window before a deleted secret is unrecoverable."
  type        = number
  default     = 7
}

variable "kms_key_arn" {
  description = "Customer-managed KMS key for the secrets. Uses the AWS-managed key when null."
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}

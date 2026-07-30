output "endpoint" {
  description = "Host and port of the database instance."
  value       = aws_db_instance.this.endpoint
}

output "address" {
  description = "Hostname of the database instance."
  value       = aws_db_instance.this.address
}

output "database_name" {
  description = "Name of the application database."
  value       = aws_db_instance.this.db_name
}

output "security_group_id" {
  description = "Security group protecting the database."
  value       = aws_security_group.this.id
}

output "credentials_secret_arn" {
  description = "ARN of the Secrets Manager entry holding the connection URL."
  value       = aws_secretsmanager_secret.credentials.arn
}

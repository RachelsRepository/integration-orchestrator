output "primary_endpoint" {
  description = "Primary endpoint address for writes."
  value       = aws_elasticache_replication_group.this.primary_endpoint_address
}

output "reader_endpoint" {
  description = "Reader endpoint address."
  value       = aws_elasticache_replication_group.this.reader_endpoint_address
}

output "connection_url" {
  description = "Redis connection URL for the application."
  value = format(
    "%s://%s:6379/0",
    var.transit_encryption_enabled ? "rediss" : "redis",
    aws_elasticache_replication_group.this.primary_endpoint_address,
  )
}

output "security_group_id" {
  description = "Security group protecting the cache."
  value       = aws_security_group.this.id
}

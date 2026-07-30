terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

locals {
  tags = merge(var.tags, { Module = "cache" })
}

resource "aws_elasticache_subnet_group" "this" {
  name       = "${var.name}-redis"
  subnet_ids = var.subnet_ids

  tags = local.tags
}

resource "aws_security_group" "this" {
  name        = "${var.name}-redis"
  description = "Redis access for the Integration Orchestrator"
  vpc_id      = var.vpc_id

  tags = merge(local.tags, { Name = "${var.name}-redis" })
}

resource "aws_vpc_security_group_ingress_rule" "redis" {
  for_each = toset(var.allowed_security_group_ids)

  security_group_id            = aws_security_group.this.id
  referenced_security_group_id = each.value
  from_port                    = 6379
  to_port                      = 6379
  ip_protocol                  = "tcp"
  description                  = "Redis from the application"
}

resource "aws_elasticache_parameter_group" "this" {
  name   = "${var.name}-redis"
  family = var.parameter_group_family

  # Circuit breaker state, rate limiter buckets and cached provider tokens are
  # all reconstructible, and every consumer already fails open when a key is
  # missing. Evicting the least recently used key under pressure is therefore
  # far better than refusing writes.
  parameter {
    name  = "maxmemory-policy"
    value = "allkeys-lru"
  }

  lifecycle {
    create_before_destroy = true
  }

  tags = local.tags
}

resource "aws_elasticache_replication_group" "this" {
  replication_group_id = "${var.name}-redis"
  description          = "Token cache, distributed locks, circuit breaker and rate limiter state"

  engine         = "redis"
  engine_version = var.engine_version
  node_type      = var.node_type
  port           = 6379

  num_cache_clusters         = var.replica_count + 1
  automatic_failover_enabled = var.replica_count > 0
  multi_az_enabled           = var.replica_count > 0

  subnet_group_name    = aws_elasticache_subnet_group.this.name
  security_group_ids   = [aws_security_group.this.id]
  parameter_group_name = aws_elasticache_parameter_group.this.name

  at_rest_encryption_enabled = true
  transit_encryption_enabled = var.transit_encryption_enabled

  # Redis holds no durable state for this platform: everything in it is a cache
  # or a lease. Snapshots would only add cost and a restore path nobody should
  # ever take.
  snapshot_retention_limit = 0

  maintenance_window = var.maintenance_window
  apply_immediately  = var.apply_immediately

  auto_minor_version_upgrade = true

  tags = merge(local.tags, { Name = "${var.name}-redis" })
}

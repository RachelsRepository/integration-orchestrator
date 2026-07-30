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
  tags = merge(var.tags, { Module = "messaging" })
}

resource "aws_security_group" "this" {
  name        = "${var.name}-kafka"
  description = "Kafka access for the Integration Orchestrator"
  vpc_id      = var.vpc_id

  tags = merge(local.tags, { Name = "${var.name}-kafka" })
}

resource "aws_vpc_security_group_ingress_rule" "kafka_tls" {
  for_each = toset(var.allowed_security_group_ids)

  security_group_id            = aws_security_group.this.id
  referenced_security_group_id = each.value
  from_port                    = 9094
  to_port                      = 9094
  ip_protocol                  = "tcp"
  description                  = "Kafka over TLS from the application"
}

resource "aws_vpc_security_group_ingress_rule" "kafka_iam" {
  for_each = toset(var.allowed_security_group_ids)

  security_group_id            = aws_security_group.this.id
  referenced_security_group_id = each.value
  from_port                    = 9098
  to_port                      = 9098
  ip_protocol                  = "tcp"
  description                  = "Kafka with IAM authentication from the application"
}

resource "aws_cloudwatch_log_group" "broker" {
  name              = "/aws/msk/${var.name}"
  retention_in_days = var.log_retention_days

  tags = local.tags
}

resource "aws_msk_configuration" "this" {
  name           = "${var.name}-config"
  kafka_versions = [var.kafka_version]

  # Topics are created explicitly rather than on first produce. Auto-creation
  # gives every typo its own topic with default partitioning, which is discovered
  # weeks later when a consumer is missing half its events.
  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    default.replication.factor=${var.default_replication_factor}
    min.insync.replicas=${var.min_insync_replicas}
    num.partitions=${var.default_partitions}
    log.retention.hours=${var.retention_hours}
    unclean.leader.election.enable=false
  PROPERTIES
}

resource "aws_msk_cluster" "this" {
  cluster_name           = var.name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.this.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_storage_gb
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  client_authentication {
    sasl {
      # IAM rather than SASL/SCRAM: it removes a password from the estate
      # entirely and ties broker access to the same task role that governs
      # everything else the service can reach.
      iam = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    encryption_at_rest_kms_key_arn = var.kms_key_arn
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.broker.name
      }
    }
  }

  tags = merge(local.tags, { Name = var.name })
}

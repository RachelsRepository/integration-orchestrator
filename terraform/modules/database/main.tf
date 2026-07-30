terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

locals {
  tags = merge(var.tags, { Module = "database" })
}

resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-db"
  subnet_ids = var.subnet_ids

  tags = local.tags
}

resource "aws_security_group" "this" {
  name        = "${var.name}-db"
  description = "PostgreSQL access for the Integration Orchestrator"
  vpc_id      = var.vpc_id

  tags = merge(local.tags, { Name = "${var.name}-db" })
}

# Ingress is granted to a security group rather than a CIDR. Task IP addresses
# change on every deployment, so a CIDR rule would either be too broad or would
# need constant maintenance.
resource "aws_vpc_security_group_ingress_rule" "postgres" {
  for_each = toset(var.allowed_security_group_ids)

  security_group_id            = aws_security_group.this.id
  referenced_security_group_id = each.value
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
  description                  = "PostgreSQL from the application"
}

resource "random_password" "master" {
  length  = 40
  special = true
  # Excluded because several AWS APIs and connection-string parsers treat these
  # as delimiters, producing failures that look like authentication problems.
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "aws_secretsmanager_secret" "credentials" {
  name                    = "${var.name}/database/master"
  description             = "Master credentials for the orchestrator database"
  recovery_window_in_days = var.secret_recovery_window_days

  tags = local.tags
}

resource "aws_secretsmanager_secret_version" "credentials" {
  secret_id = aws_secretsmanager_secret.credentials.id

  secret_string = jsonencode({
    username = var.master_username
    password = random_password.master.result
    engine   = "postgres"
    host     = aws_db_instance.this.address
    port     = aws_db_instance.this.port
    dbname   = var.database_name
    url      = "postgresql+asyncpg://${var.master_username}:${random_password.master.result}@${aws_db_instance.this.endpoint}/${var.database_name}"
  })
}

resource "aws_db_parameter_group" "this" {
  name   = "${var.name}-postgres"
  family = var.parameter_group_family

  # A server-side statement timeout is the last line of defence against one
  # pathological query holding a connection until every client gives up.
  parameter {
    name  = "statement_timeout"
    value = tostring(var.statement_timeout_ms)
  }

  parameter {
    name  = "idle_in_transaction_session_timeout"
    value = tostring(var.idle_transaction_timeout_ms)
  }

  parameter {
    name  = "log_min_duration_statement"
    value = tostring(var.log_min_duration_ms)
  }

  lifecycle {
    create_before_destroy = true
  }

  tags = local.tags
}

resource "aws_db_instance" "this" {
  identifier     = "${var.name}-postgres"
  engine         = "postgres"
  engine_version = var.engine_version
  instance_class = var.instance_class

  allocated_storage     = var.allocated_storage_gb
  max_allocated_storage = var.max_allocated_storage_gb
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id            = var.kms_key_arn

  db_name  = var.database_name
  username = var.master_username
  password = random_password.master.result
  port     = 5432

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.this.id]
  publicly_accessible    = false

  multi_az                = var.multi_az
  backup_retention_period = var.backup_retention_days
  backup_window           = var.backup_window
  maintenance_window      = var.maintenance_window
  copy_tags_to_snapshot   = true

  parameter_group_name = aws_db_parameter_group.this.name

  # Minor versions are patched automatically inside the maintenance window;
  # major versions are not, because they need application verification first.
  auto_minor_version_upgrade = true
  allow_major_version_upgrade = false
  apply_immediately           = var.apply_immediately

  performance_insights_enabled = var.performance_insights_enabled
  monitoring_interval          = var.enhanced_monitoring_interval
  monitoring_role_arn          = var.enhanced_monitoring_interval > 0 ? aws_iam_role.monitoring[0].arn : null

  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  deletion_protection       = var.deletion_protection
  skip_final_snapshot       = var.skip_final_snapshot
  final_snapshot_identifier = var.skip_final_snapshot ? null : "${var.name}-postgres-final"

  tags = merge(local.tags, { Name = "${var.name}-postgres" })
}

resource "aws_iam_role" "monitoring" {
  count = var.enhanced_monitoring_interval > 0 ? 1 : 0

  name               = "${var.name}-rds-monitoring"
  assume_role_policy = data.aws_iam_policy_document.monitoring_assume[0].json

  tags = local.tags
}

data "aws_iam_policy_document" "monitoring_assume" {
  count = var.enhanced_monitoring_interval > 0 ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["monitoring.rds.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy_attachment" "monitoring" {
  count = var.enhanced_monitoring_interval > 0 ? 1 : 0

  role       = aws_iam_role.monitoring[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

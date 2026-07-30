locals {
  name = "${var.project}-${var.environment}"

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

module "network" {
  source = "../../modules/network"

  name               = local.name
  cidr_block         = var.vpc_cidr_block
  availability_zones = var.availability_zones

  # One NAT gateway is a single point of failure and a cross-zone data charge.
  # Acceptable in a development account, not in production.
  single_nat_gateway = true

  tags = local.tags
}

# The task security group lives here rather than inside the application module.
# Both the application and every data-tier module need to reference it, and
# owning it at the root keeps the module graph acyclic.
resource "aws_security_group" "tasks" {
  name        = "${local.name}-tasks"
  description = "Integration Orchestrator ECS tasks"
  vpc_id      = module.network.vpc_id

  tags = merge(local.tags, { Name = "${local.name}-tasks" })
}

# Unrestricted egress on purpose: this service exists to call third-party
# provider APIs whose address ranges are not knowable in advance. Narrowing it
# requires an egress proxy, which is a design decision rather than a security
# group edit.
resource "aws_vpc_security_group_egress_rule" "tasks_all" {
  security_group_id = aws_security_group.tasks.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Provider APIs, AWS service endpoints and the data tier"
}

module "database" {
  source = "../../modules/database"

  name                       = local.name
  vpc_id                     = module.network.vpc_id
  subnet_ids                 = module.network.data_subnet_ids
  allowed_security_group_ids = [aws_security_group.tasks.id]

  instance_class        = var.database_instance_class
  multi_az              = false
  backup_retention_days = 7
  deletion_protection   = false
  skip_final_snapshot   = true

  tags = local.tags
}

module "cache" {
  source = "../../modules/cache"

  name                       = local.name
  vpc_id                     = module.network.vpc_id
  subnet_ids                 = module.network.data_subnet_ids
  allowed_security_group_ids = [aws_security_group.tasks.id]

  node_type     = var.cache_node_type
  replica_count = 1

  tags = local.tags
}

module "messaging" {
  source = "../../modules/messaging"

  name                       = local.name
  vpc_id                     = module.network.vpc_id
  subnet_ids                 = module.network.private_subnet_ids
  allowed_security_group_ids = [aws_security_group.tasks.id]

  broker_count               = length(var.availability_zones)
  broker_instance_type       = var.kafka_broker_instance_type
  default_replication_factor = 2
  min_insync_replicas        = 2

  tags = local.tags
}

module "secrets" {
  source = "../../modules/secrets"

  name           = local.name
  provider_slugs = var.provider_slugs

  # The database module manages its own master credentials secret; the read
  # policy has to cover it so the execution role can inject the connection URL.
  additional_secret_arns = [module.database.credentials_secret_arn]

  # Zero-day recovery lets a development environment be torn down and rebuilt
  # under the same names without waiting out the deletion window.
  recovery_window_days = 0

  tags = local.tags
}

module "application" {
  source = "../../modules/application"

  name        = local.name
  environment = "development"
  region      = var.region

  vpc_id                 = module.network.vpc_id
  public_subnet_ids      = module.network.public_subnet_ids
  private_subnet_ids     = module.network.private_subnet_ids
  task_security_group_id = aws_security_group.tasks.id

  image                  = var.image
  certificate_arn        = var.certificate_arn
  ingress_cidr_blocks    = var.ingress_cidr_blocks
  internal_load_balancer = true

  database_secret_arn     = module.database.credentials_secret_arn
  jwt_secret_arn          = module.secrets.jwt_secret_arn
  provider_secret_arns    = module.secrets.provider_secret_arns
  secrets_read_policy_arn = module.secrets.read_policy_arn

  redis_url               = module.cache.connection_url
  kafka_bootstrap_servers = module.messaging.bootstrap_brokers_sasl_iam
  kafka_resource_arns     = module.messaging.iam_resource_arns

  api_desired_count    = 1
  api_min_capacity     = 1
  worker_desired_count = 1
  worker_min_capacity  = 1

  log_level              = "DEBUG"
  log_retention_days     = 7
  otlp_endpoint          = var.otlp_endpoint
  deletion_protection    = false
  enable_execute_command = true
  alarm_topic_arns       = var.alarm_topic_arns

  tags = local.tags
}

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
  tags = merge(var.tags, { Module = "application" })

  base_environment = concat(
    [
      { name = "ENVIRONMENT", value = var.environment },
      { name = "SERVICE_NAME", value = var.name },
      { name = "LOG_LEVEL", value = var.log_level },
      { name = "KAFKA__BOOTSTRAP_SERVERS", value = var.kafka_bootstrap_servers },
      { name = "REDIS__URL", value = var.redis_url },
      { name = "PROVIDER_SANDBOX__ENABLED", value = "false" },
      { name = "PROVIDER_SANDBOX__MOUNT_IN_APP", value = "false" },
      { name = "OBSERVABILITY__TRACING_ENABLED", value = tostring(var.tracing_enabled) },
      { name = "OBSERVABILITY__OTLP_ENDPOINT", value = var.otlp_endpoint },
    ],
    var.extra_environment,
  )

  # Secrets are injected by ARN. The value never passes through Terraform, never
  # appears in state, and never lands in a task definition revision.
  base_secrets = concat(
    [
      { name = "DATABASE__URL", valueFrom = "${var.database_secret_arn}:url::" },
      { name = "JWT__PUBLIC_KEY", valueFrom = var.jwt_secret_arn },
    ],
    [
      for slug, arn in var.provider_secret_arns : {
        name      = "PROVIDERS__${upper(slug)}__CLIENT_SECRET"
        valueFrom = "${arn}:client_secret::"
      }
    ],
    [
      for slug, arn in var.provider_secret_arns : {
        name      = "PROVIDERS__${upper(slug)}__WEBHOOK_SECRET"
        valueFrom = "${arn}:webhook_secret::"
      }
    ],
  )
}

# ---------------------------------------------------------------------------
# Cluster and logging
# ---------------------------------------------------------------------------

resource "aws_ecs_cluster" "this" {
  name = var.name

  setting {
    name  = "containerInsights"
    value = var.container_insights_enabled ? "enabled" : "disabled"
  }

  tags = local.tags
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/ecs/${var.name}/api"
  retention_in_days = var.log_retention_days
  tags              = local.tags
}

resource "aws_cloudwatch_log_group" "workers" {
  name              = "/ecs/${var.name}/workers"
  retention_in_days = var.log_retention_days
  tags              = local.tags
}

resource "aws_cloudwatch_log_group" "migrate" {
  name              = "/ecs/${var.name}/migrate"
  retention_in_days = var.log_retention_days
  tags              = local.tags
}

# ---------------------------------------------------------------------------
# Networking
# ---------------------------------------------------------------------------

resource "aws_security_group" "alb" {
  name        = "${var.name}-alb"
  description = "Ingress to the Integration Orchestrator load balancer"
  vpc_id      = var.vpc_id

  tags = merge(local.tags, { Name = "${var.name}-alb" })
}

resource "aws_vpc_security_group_ingress_rule" "alb_https" {
  for_each = toset(var.ingress_cidr_blocks)

  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = each.value
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
  description       = "HTTPS from permitted networks"
}

resource "aws_vpc_security_group_egress_rule" "alb_to_tasks" {
  security_group_id            = aws_security_group.alb.id
  referenced_security_group_id = var.task_security_group_id
  from_port                    = var.container_port
  to_port                      = var.container_port
  ip_protocol                  = "tcp"
  description                  = "Forward to the API tasks"
}

# The task security group is owned by the caller rather than defined here. The data
# tier modules need to reference it in their own ingress rules, and creating it
# inside this module would make the application module depend on the database
# module and the database module depend on the application module — a cycle
# Terraform refuses to plan.
resource "aws_vpc_security_group_ingress_rule" "tasks_from_alb" {
  security_group_id            = var.task_security_group_id
  referenced_security_group_id = aws_security_group.alb.id
  from_port                    = var.container_port
  to_port                      = var.container_port
  ip_protocol                  = "tcp"
  description                  = "Traffic from the load balancer"
}

# ---------------------------------------------------------------------------
# Load balancer
# ---------------------------------------------------------------------------

resource "aws_lb" "this" {
  name               = "${var.name}-alb"
  load_balancer_type = "application"
  internal           = var.internal_load_balancer
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.public_subnet_ids

  drop_invalid_header_fields = true
  enable_deletion_protection = var.deletion_protection
  idle_timeout               = var.alb_idle_timeout_seconds

  tags = local.tags
}

resource "aws_lb_target_group" "api" {
  name        = "${var.name}-api"
  port        = var.container_port
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = var.vpc_id

  # Readiness, not liveness. A task whose database connection has dropped should
  # stop receiving traffic; the orchestrator decides separately whether to
  # restart it.
  health_check {
    path                = "/health/ready"
    matcher             = "200"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  # Long enough for an in-flight provider call to finish before the task is
  # removed, short enough not to stall a deployment.
  deregistration_delay = 30

  tags = local.tags
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.this.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = var.ssl_policy
  certificate_arn   = var.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "execution_managed" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# The execution role pulls the image and injects secrets at start-up; the task
# role is what the running process itself can do. Keeping them separate means a
# compromised application cannot read the registry credentials.
resource "aws_iam_role_policy_attachment" "execution_secrets" {
  role       = aws_iam_role.execution.name
  policy_arn = var.secrets_read_policy_arn
}

resource "aws_iam_role" "task" {
  name               = "${var.name}-task"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  tags               = local.tags
}

data "aws_iam_policy_document" "task" {
  statement {
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData",
      "kafka-cluster:DescribeGroup",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:ReadData",
    ]
    resources = var.kafka_resource_arns
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "${var.name}-task"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

resource "aws_ecs_task_definition" "api" {
  family                   = "${var.name}-api"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.api_cpu
  memory                   = var.api_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  container_definitions = jsonencode([
    {
      name        = "api"
      image       = var.image
      essential   = true
      environment = local.base_environment
      secrets     = local.base_secrets

      portMappings = [
        {
          containerPort = var.container_port
          protocol      = "tcp"
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.api.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "api"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "curl --fail --silent http://localhost:${var.container_port}/health/live || exit 1"]
        interval    = 15
        timeout     = 5
        retries     = 3
        startPeriod = 20
      }
    }
  ])

  tags = local.tags
}

resource "aws_ecs_task_definition" "workers" {
  family                   = "${var.name}-workers"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  container_definitions = jsonencode([
    {
      name        = "workers"
      image       = var.image
      essential   = true
      command     = ["python", "-m", "integration_orchestrator.workers.runner"]
      environment = local.base_environment
      secrets     = local.base_secrets

      # Long enough for a worker to finish its current batch and release its
      # claimed rows. A worker killed mid-batch leaves work that nothing looks
      # at again until it ages out.
      stopTimeout = var.worker_stop_timeout_seconds

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.workers.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "workers"
        }
      }
    }
  ])

  tags = local.tags
}

# Migrations run as their own task, invoked by the deployment pipeline before the
# services roll. Running them from a service entrypoint would have every replica
# race to migrate the same database.
resource "aws_ecs_task_definition" "migrate" {
  family                   = "${var.name}-migrate"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  container_definitions = jsonencode([
    {
      name        = "migrate"
      image       = var.image
      essential   = true
      command     = ["alembic", "upgrade", "head"]
      environment = local.base_environment
      secrets     = local.base_secrets

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.migrate.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "migrate"
        }
      }
    }
  ])

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------

resource "aws_ecs_service" "api" {
  name            = "${var.name}-api"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = var.api_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.task_security_group_id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "api"
    container_port   = var.container_port
  }

  # Rolling deployment with a circuit breaker: a task that never becomes healthy
  # rolls the deployment back instead of leaving the service half-updated.
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  deployment_maximum_percent         = 200
  deployment_minimum_healthy_percent = 100
  health_check_grace_period_seconds  = 60
  enable_execute_command             = var.enable_execute_command

  depends_on = [aws_lb_listener.https]

  tags = local.tags
}

resource "aws_ecs_service" "workers" {
  name            = "${var.name}-workers"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.workers.arn
  desired_count   = var.worker_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.task_security_group_id]
    assign_public_ip = false
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  # Workers take no inbound traffic, so replacing them one at a time is fine and
  # avoids doubling provider concurrency during a deploy.
  deployment_maximum_percent         = 200
  deployment_minimum_healthy_percent = 50
  enable_execute_command             = var.enable_execute_command

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Autoscaling
# ---------------------------------------------------------------------------

resource "aws_appautoscaling_target" "api" {
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.this.name}/${aws_ecs_service.api.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = var.api_min_capacity
  max_capacity       = var.api_max_capacity
}

resource "aws_appautoscaling_policy" "api_cpu" {
  name               = "${var.name}-api-cpu"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.api.service_namespace
  resource_id        = aws_appautoscaling_target.api.resource_id
  scalable_dimension = aws_appautoscaling_target.api.scalable_dimension

  target_tracking_scaling_policy_configuration {
    target_value = var.api_target_cpu_utilization

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }

    # Scale out quickly, scale in slowly. Shedding capacity during a lull only to
    # need it again a minute later costs more than the idle task did.
    scale_out_cooldown = 60
    scale_in_cooldown  = 300
  }
}

resource "aws_appautoscaling_target" "workers" {
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.this.name}/${aws_ecs_service.workers.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = var.worker_min_capacity
  max_capacity       = var.worker_max_capacity
}

resource "aws_appautoscaling_policy" "workers_cpu" {
  name               = "${var.name}-workers-cpu"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.workers.service_namespace
  resource_id        = aws_appautoscaling_target.workers.resource_id
  scalable_dimension = aws_appautoscaling_target.workers.scalable_dimension

  target_tracking_scaling_policy_configuration {
    target_value = var.worker_target_cpu_utilization

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }

    scale_out_cooldown = 60
    scale_in_cooldown  = 300
  }
}

# ---------------------------------------------------------------------------
# Alarms
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "api_5xx" {
  alarm_name          = "${var.name}-api-5xx"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  threshold           = var.alarm_5xx_threshold
  period              = 60
  statistic           = "Sum"
  namespace           = "AWS/ApplicationELB"
  metric_name         = "HTTPCode_Target_5XX_Count"
  treat_missing_data  = "notBreaching"

  dimensions = {
    LoadBalancer = aws_lb.this.arn_suffix
    TargetGroup  = aws_lb_target_group.api.arn_suffix
  }

  alarm_description = "The API is returning server errors."
  alarm_actions     = var.alarm_topic_arns
  ok_actions        = var.alarm_topic_arns

  tags = local.tags
}

resource "aws_cloudwatch_metric_alarm" "api_unhealthy_hosts" {
  alarm_name          = "${var.name}-api-unhealthy-hosts"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  threshold           = 0
  period              = 60
  statistic           = "Maximum"
  namespace           = "AWS/ApplicationELB"
  metric_name         = "UnHealthyHostCount"
  treat_missing_data  = "notBreaching"

  dimensions = {
    LoadBalancer = aws_lb.this.arn_suffix
    TargetGroup  = aws_lb_target_group.api.arn_suffix
  }

  alarm_description = "One or more API tasks are failing their readiness check."
  alarm_actions     = var.alarm_topic_arns

  tags = local.tags
}

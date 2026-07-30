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
  tags = merge(var.tags, { Module = "secrets" })
}

# Secret *containers* are created here; their values are not. Putting a
# credential in a Terraform variable would write it to state in plain text, and
# state is copied, backed up and read far more widely than a secret store.
# Operators populate these out of band, and the tasks read them at boot.
resource "aws_secretsmanager_secret" "provider" {
  for_each = toset(var.provider_slugs)

  name                    = "${var.name}/providers/${each.value}"
  description             = "Credentials and webhook verification material for ${each.value}"
  recovery_window_in_days = var.recovery_window_days
  kms_key_id              = var.kms_key_arn

  tags = merge(local.tags, { Provider = each.value })
}

resource "aws_secretsmanager_secret" "jwt" {
  name                    = "${var.name}/api/jwt"
  description             = "Public key material used to verify internal API bearer tokens"
  recovery_window_in_days = var.recovery_window_days
  kms_key_id              = var.kms_key_arn

  tags = local.tags
}

data "aws_iam_policy_document" "read" {
  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]
    resources = concat(
      [for secret in aws_secretsmanager_secret.provider : secret.arn],
      [aws_secretsmanager_secret.jwt.arn],
      var.additional_secret_arns,
    )
  }

  dynamic "statement" {
    for_each = var.kms_key_arn == null ? [] : [var.kms_key_arn]

    content {
      effect    = "Allow"
      actions   = ["kms:Decrypt"]
      resources = [statement.value]
    }
  }
}

resource "aws_iam_policy" "read" {
  name        = "${var.name}-read-secrets"
  description = "Allows the orchestrator tasks to read their own secrets and nothing else"
  policy      = data.aws_iam_policy_document.read.json

  tags = local.tags
}

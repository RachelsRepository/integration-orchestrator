terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }

  # State configuration is deliberately absent. `terraform init -backend-config`
  # supplies it, so the checked-in code carries no account identifiers and
  # `terraform validate` runs in CI without credentials.
  backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "integration-orchestrator"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

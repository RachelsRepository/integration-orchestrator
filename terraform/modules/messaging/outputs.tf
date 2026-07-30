output "cluster_arn" {
  description = "ARN of the MSK cluster."
  value       = aws_msk_cluster.this.arn
}

output "bootstrap_brokers_sasl_iam" {
  description = "Bootstrap servers for IAM-authenticated clients."
  value       = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
}

output "security_group_id" {
  description = "Security group protecting the brokers."
  value       = aws_security_group.this.id
}

output "iam_resource_arns" {
  description = <<-DESCRIPTION
    Resources an IAM-authenticated client needs to name in its policy. MSK
    derives topic and consumer group ARNs from the cluster ARN by substituting
    the resource type, so they are built here rather than being asked for again
    at the call site.
  DESCRIPTION
  value = [
    aws_msk_cluster.this.arn,
    "${replace(aws_msk_cluster.this.arn, ":cluster/", ":topic/")}/*",
    "${replace(aws_msk_cluster.this.arn, ":cluster/", ":group/")}/*",
  ]
}

output "expected_topics" {
  description = <<-DESCRIPTION
    Topics the outbox publisher writes to. Auto-creation is disabled, so these
    must exist before the publisher runs. They are listed rather than created
    because MSK topic management needs a client inside the VPC, which Terraform
    running outside it does not have.
  DESCRIPTION
  value = [
    "integration.request",
    "integration.provider",
  ]
}

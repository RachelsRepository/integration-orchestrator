output "provider_secret_arns" {
  description = "Map of provider slug to the ARN of its credentials secret."
  value       = { for slug, secret in aws_secretsmanager_secret.provider : slug => secret.arn }
}

output "jwt_secret_arn" {
  description = "ARN of the token verification secret."
  value       = aws_secretsmanager_secret.jwt.arn
}

output "read_policy_arn" {
  description = "Policy granting read access to exactly these secrets."
  value       = aws_iam_policy.read.arn
}

output "vpc_id" {
  description = "Identifier of the created VPC."
  value       = aws_vpc.this.id
}

output "vpc_cidr_block" {
  description = "CIDR block of the created VPC."
  value       = aws_vpc.this.cidr_block
}

output "public_subnet_ids" {
  description = "Public subnets, used by the load balancer and NAT gateways."
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "Private subnets, used by the ECS tasks."
  value       = aws_subnet.private[*].id
}

output "data_subnet_ids" {
  description = "Data subnets, used by RDS, ElastiCache and MSK."
  value       = aws_subnet.data[*].id
}

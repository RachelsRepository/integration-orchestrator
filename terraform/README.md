# Infrastructure

Illustrative Terraform for running the Integration Orchestrator on AWS. It has
never been applied against a real account, and it is included to show how the
platform would be deployed and what its runtime dependencies are, not as a
turnkey production estate.

## What it describes

| Module        | Purpose                                                                 |
| ------------- | ----------------------------------------------------------------------- |
| `network`     | VPC with public, private and data subnets across three availability zones |
| `database`    | RDS for PostgreSQL, encrypted, private, with automated backups            |
| `cache`       | ElastiCache for Redis, used for tokens, locks, circuit state, rate limits |
| `messaging`   | MSK cluster and the topics the outbox publishes to                        |
| `secrets`     | Secrets Manager entries for provider credentials and webhook keys         |
| `application` | ECS Fargate services for the API and the workers, behind an ALB           |

## Deliberate decisions

**The API and the workers are separate services.** They scale on different
signals: the API on request latency and count, the workers on outbox depth and
retry backlog. Running them in one task would force a single scaling policy on
two workloads that have nothing in common, and a burst of retries would then
compete with live traffic for the same CPU.

**Nothing that holds data is publicly routable.** RDS, ElastiCache and MSK sit in
private subnets with security groups that accept traffic only from the
application security group. The load balancer is the single ingress point.

**Secrets are references, never values.** Task definitions inject provider
credentials from Secrets Manager by ARN. No credential appears in a Terraform
variable, in state, or in an environment variable baked into an image.

**Migrations are a separate task.** Running them from the API's entrypoint means
every replica races to migrate on deploy. A standalone task runs once, and the
service waits for it.

## Layout

```
terraform/
  modules/
    network/        VPC, subnets, routing, NAT
    database/       RDS PostgreSQL
    cache/          ElastiCache Redis
    messaging/      MSK and topic configuration
    secrets/        Secrets Manager entries
    application/    ECS cluster, services, ALB, autoscaling
  environments/
    dev/            A small, single-NAT development estate
```

## Usage

The S3 backend is declared without arguments so that no account identifiers are
committed and `terraform validate` runs in CI without credentials. Supply them at
init time:

```bash
terraform -chdir=terraform/environments/dev init \
  -backend-config="bucket=my-terraform-state" \
  -backend-config="key=integration-orchestrator/dev.tfstate" \
  -backend-config="region=us-east-1"

cp terraform/environments/dev/terraform.tfvars.example \
   terraform/environments/dev/terraform.tfvars

terraform -chdir=terraform/environments/dev plan
```

`make tf-fmt` and `make tf-validate` run the checks CI runs.

## What is not covered

DNS and certificates, a WAF in front of the load balancer, cross-region
replication, an S3 backend with DynamoDB state locking, and the identity provider
that issues the API's bearer tokens. Each is environment-specific and would be
guesswork here.

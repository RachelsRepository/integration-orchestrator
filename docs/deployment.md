# Deployment

## Local stack

```bash
cp .env.example .env
make install
make up
make migrate
```

`docker-compose.yml` runs PostgreSQL, Redis, a Kafka-protocol broker (Redpanda),
the API, and the worker process. Host ports default to API `18100`, Postgres
`15433`, Redis `16380`, and Kafka `29092` to avoid collisions with other local
stacks. The provider sandbox is mounted inside the API process in local mode.

## Container image

`docker/Dockerfile` builds a multi-stage image that installs the package with
`uv` and runs as a non-root user. The same image serves both roles:

```bash
# API
docker run ... integration-orchestrator serve --host 0.0.0.0 --port 8000

# Workers
docker run ... integration-orchestrator worker
docker run ... integration-orchestrator worker --only outbox retry
```

## CI

`.github/workflows/ci.yml` runs:

1. Ruff format check and lint
2. MyPy
3. Import-linter contracts
4. Unit, contract, and e2e tests
5. Integration tests against PostgreSQL and Redis service containers
6. A migration drift check
7. A Compose **runtime** job: build/start the stack, run `scripts/compose_e2e.py`
   twice on clean volumes, run `scripts/chaos_subset.py`, capture logs on failure

## Illustrative Terraform

`terraform/` sketches an AWS deployment: VPC, RDS PostgreSQL, ElastiCache Redis,
MSK Kafka, Secrets Manager, and ECS services for the API and workers. It is
intentionally illustrative — module boundaries and IAM shape are real, but it is
not a turnkey production account.

```bash
cd terraform/environments/dev
cp terraform.tfvars.example terraform.tfvars
# configure the S3 backend, then:
terraform init -backend-config=...
terraform plan
```

Do not claim a cloud deployment succeeded unless `terraform apply` was actually
run against a real account.

## Production safety

`Settings` refuses to construct in staging/production when:

- the provider sandbox is enabled
- console log rendering is on
- Kafka is disabled
- database statement echoing is on
- JWT or provider credentials still look like local placeholders
- a provider still points at localhost

Secrets must be injected from a secrets manager. The Terraform secrets module
shows the intended shape; values themselves are never committed.

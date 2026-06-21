# Orchestrator — local setup

**Read this when:** you're setting up or running this component locally.

## Full stack (recommended)

From the meta repo root:

```bash
./setup.sh
docker compose -f deploy/docker-compose.dev.yml up --build orchestrator
```

Orchestrator edge: http://localhost:8080/orchestrator (via nginx). Requires Docker socket mount.

## This repo only

```bash
cp .env.example .env
go run ./cmd/server
```

Health: http://localhost:8080/health

## Tests (CI parity)

```bash
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from test
```

## Environment variables

See `.env.example`.

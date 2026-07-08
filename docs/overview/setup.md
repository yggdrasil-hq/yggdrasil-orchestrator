# Orchestrator — local setup

**Read this when:** you're setting up or running this component locally.

> Per ADR 003 (`../../docs/adr/003-orchestrator-kubernetes.md`), the
> Orchestrator targets a Kubernetes cluster, not a Docker socket. Locally
> that means you need *some* reachable cluster — this doc uses a throwaway
> `k3d` cluster. Bundling a cluster automatically for self-hosted installs is
> a tracked follow-up (open question #1's resolution in ADR 003), not yet
> wired into this dev flow.

## One-time: local Kubernetes cluster

```bash
brew install k3d kubectl   # or your platform's equivalent
k3d cluster create yggdrasil-dev --network yggdrasil-dev_default
mkdir -p deploy/.kube
k3d kubeconfig write yggdrasil-dev --output deploy/.kube/config-host
# The container reaches the cluster over the Docker network, not localhost:
sed 's#server: https://0.0.0.0:[0-9]*#server: https://k3d-yggdrasil-dev-serverlb:6443#' \
  deploy/.kube/config-host > deploy/.kube/config-container
```

`deploy/docker-compose.dev.yml` mounts `deploy/.kube/config-container` into
the orchestrator container and points `KUBECONFIG` at it — this file is
gitignored (`deploy/.gitignore`), regenerate it after recreating the cluster.

## Full stack (recommended)

From the meta repo root:

```bash
./setup.sh
docker compose -f deploy/docker-compose.dev.yml up --build orchestrator
```

Orchestrator edge: http://localhost:8080/orchestrator (via nginx). Requires
the local cluster above (or `KUBECONFIG`/in-cluster config pointing at some
other reachable cluster) and Postgres (`DATABASE_URL`, set automatically by
the root compose file).

## This repo only

```bash
cp .env.example .env
export KUBECONFIG=$(k3d kubeconfig write yggdrasil-dev)  # or your own cluster
go run ./cmd/server
```

Health: http://localhost:8080/health

## Tests (CI parity)

```bash
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from test
```

## Environment variables

See `.env.example`.

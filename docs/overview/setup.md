# Orchestrator — local setup

**Read this when:** you're setting up or running this component locally.

> Per ADR 003 (`../../docs/adr/003-orchestrator-kubernetes.md`), the
> Orchestrator targets a Kubernetes cluster, not a Docker socket.
> `deploy/docker-compose.dev.yml` bundles a disposable single-node **k3s**
> cluster for this (services `k3s` + `k3s-kubeconfig`) — no separate cluster
> setup needed for the full-stack dev flow below. Bundling a cluster for
> *self-hosted installs* (as opposed to this dev flow) is a separate, still
> tracked follow-up (ADR 003 §3).

## Local Kubernetes cluster (automatic)

`docker compose -f deploy/docker-compose.dev.yml up` brings up a `k3s`
service (the cluster) and a `k3s-kubeconfig` one-shot service that rewrites
k3s's self-signed kubeconfig into two forms:

- `orchestrator_kubeconfig` (a Docker volume) — reachable as `k3s:6443`,
  mounted into the `orchestrator` container and pointed to by `KUBECONFIG`.
- `deploy/.kube/config-host` — reachable as `localhost:${DEV_K3S_PORT:-6443}`,
  for your own `kubectl`/Helm from the host (e.g. the cert-manager step
  below). Gitignored (`deploy/.gitignore`); regenerated on every `up`.

Cluster state (installed charts, images pulled, etc.) persists in the
`k3s_data` volume across restarts. For a clean cluster: `docker compose -f
deploy/docker-compose.dev.yml down -v`.

To point the Orchestrator at a different cluster instead (e.g. one you
already run), remove the `k3s`/`k3s-kubeconfig` services and the
`orchestrator` service's dependency on them, and mount/set `KUBECONFIG` to
your own.

## One-time: ingress + TLS (cert-manager)

Per ADR 003 §15, primary deployments are reached through an in-cluster
ingress controller + cert-manager. k3s already bundles **Traefik** as its
default ingress controller (`kubectl get ingressclass` shows `traefik`) — no
separate install needed locally. cert-manager itself isn't bundled:

```bash
export KUBECONFIG=deploy/.kube/config-host
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
kubectl wait --for=condition=Available --timeout=120s \
  deployment/cert-manager deployment/cert-manager-webhook deployment/cert-manager-cainjector \
  -n cert-manager
```

Then apply a `selfSigned` `ClusterIssuer` — real Let's Encrypt/ACME needs a
real public domain and reachable IP, neither of which a local cluster has;
`selfSigned` proves the same TLS-wiring mechanism (Ingress → cert-manager →
issued cert) without either:

```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned-issuer
spec:
  selfSigned: {}
EOF
```

The Orchestrator's `INGRESS_CLASS_NAME` (default `traefik`) and
`CERT_ISSUER_NAME` (default `selfsigned-issuer`) match this local setup —
see `.env.example`. A self-hosted/managed install pointing at a real domain
swaps these for `ingress-nginx` and a real ACME `ClusterIssuer`; that's a
config change, not an Orchestrator code change.

## Full stack (recommended)

From the meta repo root:

```bash
./setup.sh
docker compose -f deploy/docker-compose.dev.yml up --build orchestrator
```

Orchestrator edge: http://localhost:8080/orchestrator (via nginx). The `k3s`
cluster and Postgres (`DATABASE_URL`) come up automatically as compose
dependencies — no separate cluster setup needed.

## This repo only

Bring up just the bundled cluster, then run the Orchestrator binary directly
against it:

```bash
cp .env.example .env
docker compose -f ../deploy/docker-compose.dev.yml up -d k3s-kubeconfig
export KUBECONFIG=../deploy/.kube/config-host
go run ./cmd/server
```

Health: http://localhost:8080/health

## Tests (CI parity)

```bash
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from test
```

## Environment variables

See `.env.example`.

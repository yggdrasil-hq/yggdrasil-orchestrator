# Orchestrator — local setup

**Read this when:** you're setting up or running this component locally.

> Per ADR 003 (`../../docs/adr/003-orchestrator-kubernetes.md`), the
> Orchestrator targets a Kubernetes cluster, not a Docker socket. Per ADR 016
> (`../../docs/adr/016-organization-rbac-and-cluster-routing.md`) item 13,
> there is **no bundled or instance-wide cluster** — every Organization
> configures its own target cluster (kubeconfig stored encrypted in the API's
> database, via the org settings "Kubernetes cluster" page), and the
> Orchestrator resolves each job's cluster dynamically from that at claim
> time. `deploy/docker-compose.dev.yml` does not run a cluster for you; bring
> your own (a local k3s/k3d/kind install, or any cluster you can reach).

## Local Kubernetes cluster (bring your own)

Stand up whatever single-node cluster you like — k3s, k3d, kind, Docker
Desktop's built-in Kubernetes, or a remote cluster. Then:

1. Get its kubeconfig (e.g. `sudo cat /etc/rancher/k3s/k3s.yaml` for a native
   k3s install, or `k3d kubeconfig get <cluster-name>`).
2. If the Orchestrator runs inside `deploy/docker-compose.dev.yml` and your
   cluster's API server is only reachable from the host (a kubeconfig
   `server:` of `https://127.0.0.1:6443` is the usual sign), rewrite that
   `server:` to an address reachable from inside the `orchestrator`
   container — e.g. `https://host.docker.internal:6443` (Docker Desktop) or
   your host's LAN IP.
3. Paste the (rewritten) kubeconfig into the relevant Organization's
   Kubernetes cluster settings in the Web app, or `PUT
   /organizations/:id/cluster` directly. This flips that org from
   `pending_cluster` to `ready` — required before it can create any project.

There is no `KUBECONFIG` env var to set on the Orchestrator for this path —
that variable is only a local-dev/test convenience for tools that call
`k8s.NewClient()` directly (see `internal/k8s/client.go`), not the real
per-org dispatch path.

## One-time: ingress + TLS (cert-manager)

Per ADR 003 §15, primary deployments are reached through an in-cluster
ingress controller + cert-manager. A native k3s install already bundles
**Traefik** as its default ingress controller (`kubectl get ingressclass`
shows `traefik`) — no separate install needed if that's what you're using.
cert-manager itself isn't bundled by k3s (or most other local clusters):

```bash
export KUBECONFIG=/path/to/your/cluster/kubeconfig
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

## Reaching a project's deployment locally

A project's always-on primary deployment lives at `<project-slug>.apps.<domain>`
(ADR 003 §15) — shown as a link on project home once a deploy completes (ADR
013 addendum). Two things have to be true for that link to actually load on
your machine, neither of which is automatic:

1. **The host has to resolve `<domain>`.** `APPS_BASE_DOMAIN`'s local-dev
   default is `127.0.0.1.nip.io` (`.env.example`, both this repo and
   `api/.env.example` — **must match exactly**): nip.io is a public DNS
   service that resolves any `<anything>.127.0.0.1.nip.io` to `127.0.0.1`,
   so no `/etc/hosts` editing is needed. (Only the DNS lookup leaves your
   machine — the actual HTTP(S) traffic stays local.)
2. **Something on the host has to be listening.** Whatever cluster you
   brought up needs its Traefik/ingress-nginx ingress published to a host
   port — for a native k3s install this is already true (it binds 80/443 on
   the node directly via k3s's servicelb); for k3d/kind/Docker Desktop you
   publish these yourself when creating the cluster (e.g. k3d's
   `--port 8090:80@loadbalancer` / `--port 8443:443@loadbalancer`). Set
   `orchestrator/.env`'s `APPS_BASE_DOMAIN` and `api/.env`'s
   `APPS_BASE_DOMAIN`/`APPS_HTTPS_PORT` to match whatever you published.

With both set, "Open deployment" resolves to
`https://<slug>.apps.127.0.0.1.nip.io:<your-https-port>` — expect a browser
warning on first load (the `selfSigned` `ClusterIssuer` from the previous
section issues a self-signed cert, not one a browser trusts by default);
click through it.

## Full stack (recommended)

From the meta repo root, with your cluster already up and registered against
an Organization (see above):

```bash
./setup.sh
docker compose -f deploy/docker-compose.dev.yml up --build orchestrator
```

Orchestrator edge: http://localhost:8080/orchestrator (via nginx). Postgres
(`DATABASE_URL`) comes up automatically as a compose dependency; the target
Kubernetes cluster does not — that's resolved per-organization from the API,
not from anything in this compose file.

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

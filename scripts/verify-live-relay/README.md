# Verifying the live relay's Postgres fan-out (issue #32)

> **Placement note.** This verifies the **API's** relay (`api/src/live/`), so its
> natural home is `api/scripts/verify-live-relay/`. It is committed here because
> the agent that ran #32 was scoped to `orchestrator/` while another was working in
> `api/`, and leaving it uncommitted would have made the verification
> irreproducible. Moving it is a `git mv`; issue #32 is labelled `api,
> orchestrator`, so neither repo is wrong, but the reader deserves to know why it
> is this one.

A reproducible two-replica harness. It proves the claim ADR 019 rests on — that a
socket on one API replica receives events written by another — which is
unobservable in a single-process test by construction.

## Running it

```bash
IDLE_SECONDS=90 ./run2.sh
```

Nothing else is needed: it builds the API's own test image, starts two replicas,
fronts them with nginx, creates and drops its own scratch database, and removes
every container and image it made on the way out (including on failure and on
Ctrl-C).

## Why it uses the existing dev Postgres

It runs against the dev stack's Postgres on `yggdrasil-dev_default` rather than a
container of its own, because a Postgres that already accepts connections removes
a whole class of "is the harness broken or the environment" question, and because
two replicas sharing one existing database is closer to the deployment shape being
verified. It does **not** use the operator's database: it creates `relay_verify`,
runs there, and drops it. Their data is untouched.

## What it checks

| Check | What it establishes |
|---|---|
| readiness (both replicas + nginx) | the harness is testing the relay, not a boot failure |
| stored event, same process (**control**) | without this, a cross-process pass could be caused by something else |
| stored event across replicas | **the load-bearing claim** — written by B, observed on A's socket |
| stored event across replicas (reverse) | and the other direction, so a pass is not an accident of roles |
| streaming delta across replicas | the second channel, which has its own delivery path |
| delta at the route's 4000-char limit | the documented producer bound still works |
| same 4000 chars, multi-byte | the route accepts it and the publisher drops it (issue #78) |
| through nginx | upgrade, subscribe and delivery all survive the proxy |
| idle 90s through nginx | **past nginx's default 60s `proxy_read_timeout`**, so this distinguishes the configured 3600s from the default |

Plus a `pg_notify` payload-cap probe outside the Node harness (7999 bytes accepted,
8000 rejected), which is the constraint that justifies the stored-event payload
being an id rather than event data.

## Two things that will bite you, both reproduced here first

**Replicas must not be started simultaneously.** `runMigrations` is a check-then-act
with no lock, so two replicas booting together both apply the same migration and the
loser dies with `23505` on `schema_migrations_pkey` (issue #76). `run2.sh` starts
replica A, waits for its `API listening` line, then starts B.

**A client must wait for `ready` before subscribing.** `openConnection` awaits a
session lookup before attaching its `message` listener, so a frame sent in that
window is silently dropped — no error, no `subscribed`, socket stays open and still
answers `ping` (issue #77). `verify.cjs` waits for `ready`, and the comment there
points at the issue. A client that subscribes on `open` will otherwise appear to
work and receive nothing, forever.

## Files

- `run2.sh` — orchestration, fixtures, the `pg_notify` probe, replica log dump, and cleanup.
- `verify.cjs` — the nine checks.
- `fixtures.sql` — the session / org membership / project / feature / job rows the
  socket auth path requires, all fixed UUIDs.
- `nginx.conf` — the API directives copied byte-for-byte from `deploy/nginx/dev.conf`;
  the upstream is two replicas rather than one, so this file is **not** evidence
  about the product's own routing table.

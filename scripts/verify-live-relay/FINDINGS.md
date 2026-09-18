# Live-relay Postgres fan-out: verification result (issue #32)

Harness: `run2.sh` + `verify.cjs` + `fixtures.sql` + `nginx.conf`.
Topology: two API replicas (`rv-api-a`, `rv-api-b`) on the existing
`yggdrasil-dev_default` network, sharing one Postgres, in a **scratch database**
(`relay_verify`, dropped after), behind nginx with the API directives copied from
`deploy/nginx/dev.conf`.

## Result: 9/9 checks pass

```
PASS  readiness: both replicas and nginx — all answered /health
PASS  stored event, same process (control): write api-a -> socket api-a — delivered
PASS  stored event across replicas: write api-b -> socket api-a — delivered
PASS  stored event across replicas (reverse): write api-a -> socket api-b — delivered
PASS  streaming delta across replicas: write api-b -> socket api-a — delivered
PASS  delta at the route's 4000-char limit, ASCII (~4000 bytes) — delivered
PASS  same 4000 chars, multi-byte (~12000 bytes) — route accepted (202), publisher dropped
PASS  through nginx: socket + write both via the proxy — delivered
PASS  idle 90s through nginx (default read_timeout is 60s), then delivered

checks: 9, passed: 9, failed: 0
```

The control matters: without it, a cross-process pass could be explained by
something other than the fan-out.

## The four things #32 enumerated

1. **A real NOTIFY/LISTEN round trip** — confirmed, both channels. The listener is
   a dedicated (unpooled) client, as `relay.ts` requires, and both `event` and
   `delta` payloads route to the right delivery path.
   **The 8000-byte `pg_notify` cap is real and measured:**
   ```
   7999 bytes: accepted
   8000 bytes: REJECTED → ERROR:  payload string too long
   ```
   which is exactly the constraint that makes the stored-event payload an id
   rather than event data.

2. **Two replicas delivering across each other — the load-bearing one.** Confirmed
   in **both directions**, on both channels. An event written through replica B's
   HTTP surface reaches a socket held by replica A, and vice versa. This is the
   claim that was unobservable in a single-process test by construction.

3. **The path through nginx** — confirmed. Upgrade, subscribe, event delivery and a
   **90s idle period** all survive the proxy, and 90s is past nginx's *default*
   `proxy_read_timeout` of 60s — so this distinguishes the configured 3600s from
   the default rather than merely exercising a socket that was alive anyway.

4. **A real browser WebSocket** — **not exercised.** The harness uses the `ws`
   client, not a browser. See "not verified" below.

## Two defects found by running it

### 1. Two API replicas booting simultaneously crash one another (filed)

```
error: duplicate key value violates unique constraint "schema_migrations_pkey"
code: '23505'
detail: Key (name)=(003_github_app.sql) already exists.
    at async runMigrations (/app/src/db/migrate.ts:39:5)
    at async main (/app/src/index.ts:26:3)
```
`runMigrations` reads the applied set and then applies what is missing — a
check-then-act with no lock. Two replicas starting together both see a migration
as unapplied, both apply it, and the loser dies and never serves. The harness
works around it by starting A, waiting for `API listening`, then starting B.

**This is directly relevant to #32's premise** (ADR 003 §20 commits to multiple
API replicas) even though it is not the relay: a `docker compose up` of two
replicas, or a rolling restart that overlaps, loses a replica.

### 2. A `subscribe` frame sent before `ready` is silently dropped (filed)

Reproduced with two probes against one replica:

| probe | result |
|---|---|
| subscribe **immediately on `open`** | no `subscribed`, no `error`, socket stays open, `ping`→`pong` still works |
| subscribe **after receiving `ready`** | `subscribed` in 4 ms |

`openConnection` awaits a session lookup *before* attaching `socket.on("message")`,
so a frame arriving in that window is lost — `ws` emits `message` only to already
registered listeners, and there are none. The protocol therefore has an implicit
"wait for `ready`" handshake that is **not enforced, not on the wire, and fails
silently**: the socket looks healthy and answers `ping` while receiving no events.

This is precisely the failure mode #32 warns about — "the client falls back to 2 s
polling if the socket fails, which means a broken socket path degrades gracefully
into looking fine" — except worse here, because the socket does *not* fail, so
nothing triggers the fallback. The Web app happens to wait for `ready`, which is
why it has never surfaced.

### 3. Two caps in different units (filed, minor)

The internal route bounds a delta at **4000 characters** "because it travels
through a `pg_notify` payload whose hard limit is 8000 bytes"
(`jobs/internal-routes.ts`), while the publisher bounds it at **7000 bytes**
(`live/types.ts`). 4000 characters of 3-byte text is 12000 bytes, so such a
payload clears the route (202) and is then dropped by the publisher with only a
log line — measured, not inferred:

```
PASS  same 4000 chars, multi-byte (~12000 bytes) — route accepted (202), publisher dropped
```

The route's stated intent (bound the payload for `pg_notify`) therefore does not
hold for multi-byte text.

## What was NOT verified, and why

- **A real browser WebSocket.** The harness uses the `ws` Node client. The client
  behaviour #32 flags — silent degradation to 2 s polling — lives in the Web app's
  client code, which a `ws` client does not reproduce. Verifying it needs
  `playwright-cli` against `localhost:8080` with the dev stack pointed at two
  replicas, which also means changing the product's nginx upstream (a single `api`
  service) — an operator-visible change I did not make. **The socket path itself
  is verified; the browser's degradation behaviour is not.**
- **A genuinely multi-replica *deployment*.** Both replicas were started by this
  harness, not by `docker compose` with a replica count. That is the right shape
  for exercising the fan-out, and it is not the same thing as proving a scaled
  deployment's rollout.
- **Reconnect across a replica death.** Not attempted.

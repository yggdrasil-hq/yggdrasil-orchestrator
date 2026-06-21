# CLAUDE.md — Orchestrator

> Canonical agent guide for this repo. `AGENTS.md` is a thin pointer to this
> file. This repo is a **git submodule** of the Yggdrasil meta repo.

## Context discipline (read first)

1. This router is short on purpose. Find your task in the routing table and open
   **only** the doc(s) it points to.
2. Each doc starts with `**Read this when:**` — use it to decide before reading.
3. For **suite-wide** context (product, architecture, glossary, cross-component
   contracts), don't restate it here — follow the up-links to the meta repo.

## What this repo is

Yggdrasil's stateless job executor. Receives a job spec from the
API, provisions an ephemeral Docker container, runs the Pi coding agent,
streams events back, and tears down when done. Owns no durable state between runs.

- **Role in Yggdrasil:** Orchestrator
- **Stack:** Go, Docker socket integration, Pi agent RPC/SDK (planned)
- **Talks to:** API (job specs + streamed events), GitHub (clone, branch, PR via scoped token)

## Suite-wide context (up-links to the meta repo)

> These resolve when this repo is checked out inside the Yggdrasil meta repo.

- Product & scope: `../docs/overview/product.md`
- Architecture / data flow: `../docs/overview/architecture.md`
- Glossary: `../docs/overview/glossary.md`
- This component's bridge page: `../docs/components/orchestrator.md`
- Pi agent integration: `../docs/concepts/pi-agent.md`
- Job dispatch contract: `../docs/concepts/job-dispatch.md`
- Roadmap & open questions: `../docs/roadmap/`

## Routing table (this repo)

| If your task is about…        | Read                          |
|-------------------------------|-------------------------------|
| Orientation / how it's built  | `docs/overview/architecture.md` |
| Local dev / setup             | `docs/overview/setup.md`      |
| Conventions specific to here  | `docs/conventions/`           |

Full index: `docs/README.md`.

## Standing rules

- Keep docs in sync with code in the same change.
- One concern per doc; route, don't dump. (See the meta repo's
  `../docs/conventions/documentation-guide.md`.)
- Suite-wide facts live in the meta repo — link up, don't copy.

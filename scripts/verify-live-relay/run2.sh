#!/usr/bin/env bash
# Two-replica live-relay fan-out verification (issue #32).
#
# Uses the EXISTING dev stack's Postgres (on yggdrasil-dev_default), per the
# coordinator's guidance: that removes the question of whether a fresh Postgres
# will accept a connection, and it is closer to what #32 asks — two API replicas
# sharing one database. The operator's own database is NOT used: this creates and
# uses a scratch database (`relay_verify`), so nothing lands in their data.
#
# The API image is the repo's own test image, so the code under test is the repo's.
set -uo pipefail

REPO=/home/mugiwara/files/personal/projects/apps/yggdrasil
API_DIR="$REPO/api"
HERE=/tmp/relay-verify
NET=yggdrasil-dev_default
PG=yggdrasil-dev-postgres-1
DB=relay_verify
A=rv-api-a
B=rv-api-b
NGINX=rv-nginx
IMAGE=rv-api:verify
TOKEN=relay-verify-token
IDLE_SECONDS="${IDLE_SECONDS:-90}"
PGURL_HOST="postgresql://yggdrasil:change-me@postgres:5432/$DB"

cleanup() {
  set +e
  echo ""
  echo "== cleanup =="
  for c in "$A" "$B" "$NGINX"; do
    docker rm -f "$c" >/dev/null 2>&1 && echo "removed container $c"
  done
  docker image rm -f "$IMAGE" >/dev/null 2>&1 && echo "removed image $IMAGE"
  if docker exec "$PG" psql -U yggdrasil -d postgres -c "DROP DATABASE IF EXISTS $DB" >/dev/null 2>&1; then
    echo "dropped scratch database $DB"
  else
    echo "COULD NOT DROP scratch database $DB — needs dropping by hand"
  fi
  echo "leftover rv-* objects:"
  docker ps -a --format '{{.Names}}' | grep -E '^rv-' || echo "  (none)"
  docker image ls --format '{{.Repository}}' | grep -E '^rv-' || echo "  (no rv- images)"
}
trap cleanup EXIT INT TERM

echo "== scratch database (in the existing dev Postgres; operator's DB untouched) =="
docker exec "$PG" psql -U yggdrasil -d postgres -c "DROP DATABASE IF EXISTS $DB" >/dev/null 2>&1
docker exec "$PG" psql -U yggdrasil -d postgres -c "CREATE DATABASE $DB" >/dev/null
echo "created $DB"

echo "== api image (repo's own test image) =="
docker build -q -t "$IMAGE" -f "$API_DIR/deploy/Dockerfile.test" "$API_DIR" >/dev/null
echo "built $IMAGE"

# A valid 32-byte base64 value, generated here rather than read from the
# operator's configuration, so this harness carries none of their values. Held
# in a variable rather than written to disk, so it leaves no file behind.
KEY="$(docker run --rm "$IMAGE" node -e 'process.stdout.write(require("crypto").randomBytes(32).toString("base64"))')"

start_replica() {
  local name="$1"
  # NODE_ENV must not be "test": index.ts calls main() only when it is not, and the
  # test image sets NODE_ENV=test for its own entrypoint.
  docker run -d --name "$name" --network "$NET" \
    -e NODE_ENV=production \
    -e PORT=3000 \
    -e DATABASE_URL="$PGURL_HOST" \
    -e SECRETS_ENCRYPTION_KEY="$KEY" \
    -e INTERNAL_API_TOKEN="$TOKEN" \
    -e APP_PUBLIC_URL="http://localhost:8080/app" \
    -e API_PUBLIC_URL="http://localhost:8080/api" \
    -e TEST_SCHEDULER_ENABLED=false \
    "$IMAGE" ./node_modules/.bin/tsx src/index.ts >/dev/null
}

echo "== two api replicas, one database =="
# Serialised deliberately, and this is NOT cosmetic. Two replicas booting
# simultaneously both see the same migration as unapplied and both apply it; the
# loser dies on `schema_migrations_pkey` and never serves. That is a real defect
# (filed separately) in `src/db/migrate.ts`'s check-then-act, not a property of
# this harness — but it is not what #32 is about, so the harness works around it
# by letting the first replica finish migrating before starting the second. A real
# rolling deploy tolerates this because replicas do not usually start in the same
# millisecond; a `docker compose up` of two at once does not.
start_replica "$A"
echo "started $A; waiting for it to finish migrating before starting $B"
for _ in $(seq 1 120); do
  if docker logs "$A" 2>&1 | grep -q "API listening"; then break; fi
  sleep 1
done
start_replica "$B"
echo "started $B"

echo "== nginx (two-replica upstream; API directives copied from deploy/nginx/dev.conf) =="
docker run -d --name "$NGINX" --network "$NET" \
  -v "$HERE/nginx.conf:/etc/nginx/conf.d/default.conf:ro" \
  nginx:alpine >/dev/null
echo "started $NGINX"

echo "== waiting for migrations (first replica to boot applies them) =="
for _ in $(seq 1 120); do
  if docker exec "$PG" psql -U yggdrasil -d "$DB" -tAc \
      "select to_regclass('public.sessions') is not null" 2>/dev/null | grep -q t; then
    break
  fi
  sleep 1
done
docker exec "$PG" psql -U yggdrasil -d "$DB" -tAc "select count(*) from schema_migrations" 2>/dev/null \
  | sed 's/^/  migrations applied: /'

echo "== fixtures (fixed UUIDs; see fixtures.sql) =="
docker exec -i "$PG" psql -U yggdrasil -d "$DB" -q < "$HERE/fixtures.sql" && echo "inserted"

echo ""
echo "== pg_notify payload cap (the reason the payload is an id, not the event) =="
# On a throwaway channel: the 8000-byte limit is a property of pg_notify itself,
# and using 'job_events' would make every relay listener parse it as an event id."
# pg_notify returns void, so it cannot be wrapped in length() — the question is
# only whether the call raises, and Postgres caps the payload at 8000 bytes.
for bytes in 7999 8000; do
  printf "  %s bytes: " "$bytes"
  out="$(docker exec "$PG" psql -U yggdrasil -d "$DB" -tAc \
    "do \$\$ begin perform pg_notify('relay_verify_cap_probe', repeat('x', $bytes)); end \$\$;" 2>&1)"
  if echo "$out" | grep -q ERROR; then
    echo "REJECTED → $(echo "$out" | grep -oE 'ERROR:.*' | head -1)"
  else
    echo "accepted"
  fi
done

echo ""
echo "== replica logs (both, so a startup failure is visible) =="
for c in "$A" "$B"; do
  echo "--- $c (last 20 lines) ---"
  docker logs "$c" 2>&1 | tail -20 | cut -c1-240 | sed 's/^/  /'
  if ! docker ps --format '{{.Names}}' | grep -qx "$c"; then
    echo "  !! $c is NOT RUNNING (exit code $(docker inspect "$c" --format '{{.State.ExitCode}}' 2>/dev/null))"
  fi
done

echo ""
echo "== verification =="
docker run --rm --network "$NET" \
  -e INTERNAL_API_TOKEN="$TOKEN" \
  -e IDLE_SECONDS="$IDLE_SECONDS" \
  -v "$HERE/verify.cjs:/app/verify.cjs:ro" \
  "$IMAGE" node /app/verify.cjs

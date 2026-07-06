#!/usr/bin/env bash
#
# Run spicebench TPC-H in BOOTSTRAP mode against a local PostgreSQL (WAL CDC),
# collecting time-series metrics (spiced CPU/RSS, PostgreSQL stats, ETL throughput).
# Mirrors run-spicebench-local-mongo.sh for direct performance comparison — same
# bootstrap workload (seed base -> snapshot -> rate-limited pure mutations), the
# only difference is the source/sink is PostgreSQL WAL instead of MongoDB streams.
#
# All state is local — no S3/MinIO required:
#   - Data is generated once and cached at DATA_ARCHIVE (generated if missing)
#   - PostgreSQL is running (wal_level=logical)
#   - spiced is launched by spidapter
#
# Usage:
#   ./scripts/run-spicebench-local-postgres.sh [--help]
#
# Configuration (environment variables, with defaults):
#   SF=1                        TPC-H scale factor
#   NUM_STEPS=25                base steps (creates-only) before mutation phase
#   BOOTSTRAP_MUTATION_STEPS=10 pure-mutation steps after the base snapshot
#   BOOTSTRAP_CHURN_FRACTION=0.17  fraction of rows churned per mutation step
#   UPDATE_RATIO=0.8  DELETE_RATIO=0.2  split of churn between updates and deletes
#   CHECKPOINT_INTERVAL_STEPS=2  steps between generated checkpoints
#   DATA_ARCHIVE=./data/spicebench-bootstrap  BASE path for the cached data archive;
#       a parameter signature (sf/ns/ms/cf/ur/dr) is appended before .tar.zst, so
#       changing any generation knob regenerates instead of reusing a stale cache
#   SPICEBENCH=./target/release/spicebench      path to spicebench binary
#   SPICEAI_REPO=../spiceai   spiceai checkout (sibling of spicebench by default)
#   SPIDAPTER_MANIFEST=$SPICEAI_REPO/tools/spidapter/Cargo.toml
#   SPICED=$SPICEAI_REPO/target/release/spiced  path to spiced binary
#   SCENARIO_BASE_PATH=./scripts/local-scenarios  bundled local-compute scenarios
#   PG_HOST=localhost  PG_PORT=5432  PG_USER=postgres  PG_PASSWORD=  PG_DATABASE=spicebench
#   READY_WAIT=60               seconds spidapter waits for spiced readiness
#   NO_TEARDOWN=false           keep per-run state after the run
#   SPICED_LOG=info,...         log filter passed through to the spiced child
#   OUTDIR=/tmp/spicebench-postgres-<timestamp>  output directory for logs + metrics
#   CHECKPOINT_DIR=./data/spicebench-checkpoints-bootstrap  BASE checkpoint dir; the
#       signature plus checkpoint cadence (ci) is appended, so it regenerates when
#       any generation knob OR CHECKPOINT_INTERVAL_STEPS changes
#   VALIDATION_TIMEOUT=3600   max seconds to wait for checkpoint convergence
#   RUST_LOG=info,etl::sink::adbc=debug

set -uo pipefail
# Put the script in its own process group so Ctrl+C can kill all children
set -m 2>/dev/null || true

case "${1:-}" in -h|--help) sed -n '2,41p' "$0"; exit 0;; esac

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

SF="${SF:-1}"
# Bootstrap dataset shape (identical knobs to run-spicebench-local-mongo.sh, so
# the same SF/BOOTSTRAP_*/UPDATE_RATIO/DELETE_RATIO/CHECKPOINT_INTERVAL_STEPS env
# produce the same workload against PostgreSQL).
NUM_STEPS="${NUM_STEPS:-25}"                               # base steps (creates-only)
BOOTSTRAP_MUTATION_STEPS="${BOOTSTRAP_MUTATION_STEPS:-10}" # pure-mutation steps after the base
BOOTSTRAP_CHURN_FRACTION="${BOOTSTRAP_CHURN_FRACTION:-0.17}"
UPDATE_RATIO="${UPDATE_RATIO:-0.8}"
DELETE_RATIO="${DELETE_RATIO:-0.2}"
CHECKPOINT_INTERVAL_STEPS="${CHECKPOINT_INTERVAL_STEPS:-2}" # checkpoint cadence over mutation steps

# Parameter signature baked into the archive/checkpoint names so changing ANY
# generation knob yields a NEW path -> automatic cache miss -> regeneration. Old
# artifacts coexist (named by their params) instead of being silently reused or
# clobbered, so you can compare runs side by side.
#   GEN_SIG  = everything that determines the data archive (SF + generate knobs)
#   CKPT_SIG = the archive params PLUS the checkpoint cadence. Checkpoints derive
#              from the archive, so they must also invalidate when a data param
#              changes — hence CKPT_SIG is a superset of GEN_SIG.
GEN_SIG="sf${SF}-ns${NUM_STEPS}-ms${BOOTSTRAP_MUTATION_STEPS}-cf${BOOTSTRAP_CHURN_FRACTION}-ur${UPDATE_RATIO}-dr${DELETE_RATIO}"
CKPT_SIG="${GEN_SIG}-ci${CHECKPOINT_INTERVAL_STEPS}"

# DATA_ARCHIVE / CHECKPOINT_DIR are treated as a BASE location; the signature is
# always appended. Override the base to relocate the artifacts — the param suffix
# still applies, so the regenerate-on-param-change guarantee holds either way.
# (For the archive the suffix is inserted before the .tar.zst extension.)
DATA_ARCHIVE_BASE="${DATA_ARCHIVE:-$REPO_ROOT/data/spicebench-bootstrap}"
CHECKPOINT_DIR_BASE="${CHECKPOINT_DIR:-$REPO_ROOT/data/spicebench-checkpoints-bootstrap}"
DATA_ARCHIVE="${DATA_ARCHIVE_BASE%.tar.zst}-${GEN_SIG}.tar.zst"
CHECKPOINT_DIR="${CHECKPOINT_DIR_BASE%/}-${CKPT_SIG}"
SPICEBENCH="${SPICEBENCH:-$REPO_ROOT/target/release/spicebench}"
# Note: binary is built with --features duckdb (required for checkpoint generation)
# spiceai is expected as a sibling checkout of spicebench (../spiceai); override
# SPICEAI_REPO to point elsewhere.
SPICEAI_REPO="${SPICEAI_REPO:-$(cd "$REPO_ROOT/.." && pwd)/spiceai}"
SPIDAPTER_MANIFEST="${SPIDAPTER_MANIFEST:-$SPICEAI_REPO/tools/spidapter/Cargo.toml}"
SPICED="${SPICED:-$SPICEAI_REPO/target/release/spiced}"
# Local runs use the local-compute scenarios (compute: local) that ship with
# spicebench under scripts/local-scenarios. The CI variants (compute: scp) live
# in the spidapter image at tools/spidapter/scenarios, so don't default to those.
SCENARIO_BASE_PATH="${SCENARIO_BASE_PATH:-$REPO_ROOT/scripts/local-scenarios}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-}"
PG_DATABASE="${PG_DATABASE:-spicebench}"
OUTDIR="${OUTDIR:-/tmp/spicebench-postgres-$(date +%Y%m%d-%H%M%S)}"
VALIDATION_TIMEOUT="${VALIDATION_TIMEOUT:-3600}"
# Seconds spidapter waits for spiced to become ready after activate.
READY_WAIT="${READY_WAIT:-1800}"
# Set NO_TEARDOWN=true to KEEP per-run state (slots/schemas) after the run.
NO_TEARDOWN="${NO_TEARDOWN:-false}"
RUST_LOG="${RUST_LOG:-info,etl::sink::adbc=debug}"
# spiced log filter (passed through to the spiced child via SPICED_LOG).
SPICED_LOG="${SPICED_LOG:-info}"
PSQL="${PSQL:-$(command -v psql 2>/dev/null || echo /opt/homebrew/bin/psql)}"

if [ -n "${PG_PASSWORD:-}" ]; then
  PG_DSN="host=${PG_HOST} port=${PG_PORT} user=${PG_USER} password=${PG_PASSWORD} dbname=${PG_DATABASE} sslmode=disable"
else
  PG_DSN="host=${PG_HOST} port=${PG_PORT} user=${PG_USER} dbname=${PG_DATABASE} sslmode=disable"
fi

# Bootstrap mutations are updates + deletes (upserts), so use the staging-table
# update strategy for the PostgreSQL ADBC sink.
SPICEBENCH_ADBC_UPDATE_STRATEGY="${SPICEBENCH_ADBC_UPDATE_STRATEGY:-staging_table}"

METRICS_PORT="${METRICS_PORT:-19090}"

mkdir -p "$OUTDIR"
LOG="$OUTDIR/spicebench.log"

echo "============================================"
echo " spicebench local PostgreSQL WAL run (BOOTSTRAP mode)"
echo "  SF=$SF  base_steps=$NUM_STEPS  mutation_steps=$BOOTSTRAP_MUTATION_STEPS"
echo "  churn=$BOOTSTRAP_CHURN_FRACTION  update/delete=$UPDATE_RATIO/$DELETE_RATIO"
echo "  checkpoint_interval=$CHECKPOINT_INTERVAL_STEPS"
echo "  archive=$DATA_ARCHIVE"
echo "  checkpoints=$CHECKPOINT_DIR"
echo "  output=$OUTDIR"
echo "============================================"

# ---------------------------------------------------------------------------
# Step 1: Build spicebench if binary is missing or stale
# ---------------------------------------------------------------------------
# `--features duckdb` is only needed to GENERATE checkpoints. When checkpoints
# are already cached, the run uses the ADBC sink and doesn't need duckdb — and
# we skip the feature to avoid the workspace arrow-58 vs duckdb-fork arrow-57
# build conflict. Build with duckdb only when we actually have to checkpoint.
echo ""
echo "[1/4] Building spicebench..."
if [ -f "$CHECKPOINT_DIR/checkpoints.json" ]; then
  BUILD_FEATURES=""
else
  BUILD_FEATURES="--features duckdb"
fi
RUSTC_WRAPPER="" cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml" $BUILD_FEATURES
echo "      done: $SPICEBENCH"

# ---------------------------------------------------------------------------
# Step 2: Generate data archive if not cached
# ---------------------------------------------------------------------------
echo ""
if [ -f "$DATA_ARCHIVE" ]; then
  echo "[2/4] Using cached data archive: $DATA_ARCHIVE"
else
  echo "[2/4] Generating SF${SF} BOOTSTRAP data (full base + $BOOTSTRAP_MUTATION_STEPS mutation steps)..."
  RUSTC_WRAPPER="" "$SPICEBENCH" generate \
    --scale-factor "$SF" \
    --num-steps "$NUM_STEPS" \
    --dataset tpch \
    --scenario tpch \
    --bootstrap \
    --bootstrap-mutation-steps "$BOOTSTRAP_MUTATION_STEPS" \
    --bootstrap-churn-fraction "$BOOTSTRAP_CHURN_FRACTION" \
    --update-ratio "$UPDATE_RATIO" \
    --delete-ratio "$DELETE_RATIO" \
    --output-archive "$DATA_ARCHIVE"
  echo "      saved to $DATA_ARCHIVE"
fi

# Generate checkpoints if not cached — required for validation
if [ -f "$CHECKPOINT_DIR/checkpoints.json" ]; then
  echo "[2b/4] Using cached checkpoints: $CHECKPOINT_DIR"
else
  echo "[2b/4] Generating bootstrap checkpoints from local archive (cp0 = full base, then per mutation interval)..."
  DUCKDB_PATH="/tmp/spicebench-checkpoint-${CKPT_SIG}.duckdb"
  rm -f "$DUCKDB_PATH"
  mkdir -p "$CHECKPOINT_DIR"
  if ! RUSTC_WRAPPER="" "$SPICEBENCH" checkpoint \
    --scenario tpch \
    --version "${SF}.0" \
    --duckdb-path "$DUCKDB_PATH" \
    --checkpoint-dir "$CHECKPOINT_DIR" \
    --checkpoint-interval-steps "$CHECKPOINT_INTERVAL_STEPS" \
    --etl-source-archive "$DATA_ARCHIVE"; then
    echo "ERROR: checkpoint generation failed"
    exit 1
  fi
  echo "      saved to $CHECKPOINT_DIR"
fi

# ---------------------------------------------------------------------------
# Step 3: Ensure PostgreSQL is running and clean
# ---------------------------------------------------------------------------
echo ""
echo "[3/4] Checking local PostgreSQL..."
if ! "$PSQL" "$PG_DSN" -c "SELECT 1" >/dev/null 2>&1; then
  echo "ERROR: cannot connect to PostgreSQL at ${PG_HOST}:${PG_PORT}"
  echo "       Start PostgreSQL: brew services start postgresql@17"
  echo "       Ensure wal_level=logical in /opt/homebrew/var/postgresql@17/postgresql.conf"
  exit 1
fi

# Verify wal_level is logical (required for WAL CDC)
WAL_LEVEL=$("$PSQL" "$PG_DSN" -t -c "SHOW wal_level" 2>/dev/null | tr -d ' ')
if [ "$WAL_LEVEL" != "logical" ]; then
  echo "ERROR: wal_level=${WAL_LEVEL}, must be 'logical'"
  echo "       Add 'wal_level = logical' to /opt/homebrew/var/postgresql@17/postgresql.conf"
  echo "       then: brew services restart postgresql@17"
  exit 1
fi

# Ensure the target database exists
"$PSQL" "host=${PG_HOST} port=${PG_PORT} user=${PG_USER} dbname=template1 sslmode=disable" \
  -c "CREATE DATABASE ${PG_DATABASE}" >/dev/null 2>&1 || true
echo "      PostgreSQL reachable"

# Drop leftover replication slots from previous runs.
# spidapter creates slots named spice_<table>_<hash>. With max_replication_slots=10
# and 8 TPC-H tables per run, leftover slots quickly exhaust the limit.
echo "      dropping stale replication slots..."
"$PSQL" "$PG_DSN" -t -c "
  SELECT slot_name FROM pg_replication_slots
  WHERE slot_name LIKE 'spice%' OR slot_name LIKE 'spicebench%'
" 2>/dev/null | while read -r slot; do
  slot=$(echo "$slot" | tr -d ' ')
  [ -z "$slot" ] && continue
  echo "        dropping slot: $slot"
  # Terminate any active WAL sender using this slot first
  "$PSQL" "$PG_DSN" -c "
    SELECT pg_terminate_backend(active_pid)
    FROM pg_replication_slots
    WHERE slot_name = '$slot' AND active_pid IS NOT NULL
  " >/dev/null 2>&1 || true
  "$PSQL" "$PG_DSN" -c "SELECT pg_drop_replication_slot('$slot')" >/dev/null 2>&1 || true
done

# Drop all TPC-H schemas for a clean slate
echo "      dropping TPC-H schemas..."
"$PSQL" "$PG_DSN" -t -c "
  SELECT 'DROP SCHEMA ' || schema_name || ' CASCADE;'
  FROM information_schema.schemata
  WHERE schema_name LIKE 'tpch_%'
" 2>/dev/null | "$PSQL" "$PG_DSN" >/dev/null 2>&1 || true

# Confirm slots are clear
SLOT_COUNT=$("$PSQL" "$PG_DSN" -t -c "
  SELECT COUNT(*) FROM pg_replication_slots
  WHERE slot_name LIKE 'spice%' OR slot_name LIKE 'spicebench%'
" 2>/dev/null | tr -d ' ')
echo "      replication slots remaining: ${SLOT_COUNT:-?}"
echo "      PostgreSQL ready"

cleanup() {
  echo ""
  # Kill the spicebench process group so spidapter + spiced also exit
  kill -- -$$ 2>/dev/null || true
  exit 130
}
trap cleanup INT TERM

# ---------------------------------------------------------------------------
# Step 4: Run spicebench
# ---------------------------------------------------------------------------
echo ""
echo "[4/4] Running spicebench..."
echo "      logs -> $LOG"
echo ""

NO_TEARDOWN_ARG=""
[ "$NO_TEARDOWN" = "true" ] && NO_TEARDOWN_ARG="--no-teardown"

RUSTC_WRAPPER="" \
SPICEBENCH_TARGET_BATCH_ROWS=100000 \
SPICEBENCH_ADBC_UPDATE_STRATEGY="$SPICEBENCH_ADBC_UPDATE_STRATEGY" \
SPICEBENCH_ADBC_DELETE_BATCH_SIZE=5000 \
SPICEBENCH_ADBC_REUSE_BULK_INGEST_STREAMS=false \
SPICEBENCH_ADBC_ANALYZE_STAGING_BEFORE_MERGE=true \
SPICEBENCH_SINK_PARALLELISM_PER_TABLE="${SINK_PARALLELISM_PER_TABLE:-8}" \
SPICEBENCH_SINK_CHUNK_ROWS=40000 \
SPICEBENCH_SINK_PARALLELISM="${SINK_PARALLELISM:-8}" \
RUST_LOG="$RUST_LOG" \
  "$SPICEBENCH" run \
    --scenario tpch \
    --scale-factor "$SF" \
    --etl-sink adbc \
    --etl-source-archive "$DATA_ARCHIVE" \
    --bootstrap \
    --validate-results \
    --scrape-sut-metrics \
    --checkpoint-local-dir "$CHECKPOINT_DIR" \
    --checkpoint-validation-timeout "$VALIDATION_TIMEOUT" \
    ${NO_TEARDOWN_ARG} \
    --system-adapter-stdio-cmd cargo \
    --system-adapter-stdio-args "run --manifest-path $SPIDAPTER_MANIFEST -- stdio \
      --ready-wait $READY_WAIT \
      --scenario postgres-wal \
      --scenario-base-path $SCENARIO_BASE_PATH \
      --spiced-binary $SPICED" \
    --system-adapter-env "SPIDAPTER_METRICS_PORT=${METRICS_PORT}" \
    --system-adapter-env "PG_HOST=${PG_HOST}" \
    --system-adapter-env "PG_PORT=${PG_PORT}" \
    --system-adapter-env "PG_USER=${PG_USER}" \
    --system-adapter-env "PG_PASSWORD=${PG_PASSWORD:-none}" \
    --system-adapter-env "PG_DATABASE=${PG_DATABASE}" \
    --system-adapter-env "SPICED_LOG=$SPICED_LOG" \
  2>&1 | tee "$LOG"
BENCH_EXIT=$?

echo ""
echo "===== Run outcome ====="
grep -iE "outcome|validation|checkpoint|passed|failed|pipeline_failure" "$LOG" \
  | tail -10

echo ""
echo "===== Replication slot cleanup check ====="
LEFTOVER=$("$PSQL" "$PG_DSN" -t -c "
  SELECT slot_name FROM pg_replication_slots
  WHERE slot_name LIKE 'spice%' OR slot_name LIKE 'spicebench%'
" 2>/dev/null | grep -v '^$' || true)
if [ -n "$LEFTOVER" ]; then
  echo "  WARNING: leftover slots (teardown may not have run):"
  echo "$LEFTOVER" | while read -r s; do echo "    $s"; done
  echo "  Run: psql \"$PG_DSN\" -c \"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'spice%';\""
else
  echo "  OK: no leftover replication slots"
fi

echo ""
echo "DONE  exit=$BENCH_EXIT  output=$OUTDIR"
exit "$BENCH_EXIT"

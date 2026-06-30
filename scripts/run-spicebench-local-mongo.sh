#!/usr/bin/env bash
#
# Run spicebench in BOOTSTRAP MODE against a local MongoDB replica set,
# collecting time-series metrics (spiced CPU/RSS, MongoDB row counts, ETL throughput).
#
# Bootstrap mode models CDC onboarding:
#   1. seed the full base dataset into MongoDB (unthrottled, SUT not yet started)
#   2. start spiced via spidapter's `activate` RPC, which snapshots the base
#   3. validate the snapshot against checkpoint 0 (the full base) via the oracle
#   4. stream pure mutations (updates/deletes, rate-limited) and validate each
#      mutation checkpoint (cp1+) against the oracle
#
# All state is local — no S3/MinIO required:
#   - Bootstrap data is generated once and cached at DATA_ARCHIVE
#   - Bootstrap checkpoints (cp0 = full base, cp1+ per mutation interval) are
#     generated once and cached at CHECKPOINT_DIR
#   - MongoDB is running locally as a replica set (required for Change Streams)
#   - spiced is launched by spidapter
#
# REQUIRES: a spidapter checkout that supports the `activate` RPC (the bootstrap
# setup/activate split) — i.e. the spiceai spidapter bootstrap branch.
#
# Usage:
#   ./scripts/run-spicebench-local-mongo.sh [--help]
#
# Configuration (environment variables, with defaults):
#   SF=1                      TPC-H scale factor
#   NUM_STEPS=25              base dataset steps (creates-only)
#   BOOTSTRAP_MUTATION_STEPS=10  pure-mutation steps appended after the base
#   BOOTSTRAP_CHURN_FRACTION=0.17  total fraction of the base mutated across all steps
#   UPDATE_RATIO=0.8          fraction of mutations that are updates
#   DELETE_RATIO=0.2          fraction of mutations that are deletes
#   CHECKPOINT_INTERVAL_STEPS=2  checkpoint cadence over the mutation steps
#   DATA_ARCHIVE=./data/spicebench-bootstrap  BASE path for the data archive; a
#       parameter signature (sf/ns/ms/cf/ur/dr) is appended before .tar.zst, so
#       changing any generation knob regenerates instead of reusing a stale cache
#   CHECKPOINT_DIR=./data/spicebench-checkpoints-bootstrap  BASE checkpoint dir; the
#       signature plus checkpoint cadence (ci) is appended, so it regenerates when
#       any generation knob OR CHECKPOINT_INTERVAL_STEPS changes
#   SPICEBENCH=./target/release/spicebench    path to spicebench binary
#   SPICEAI_REPO=../spiceai   spiceai checkout (sibling of spicebench by default)
#   SPIDAPTER_MANIFEST=$SPICEAI_REPO/tools/spidapter/Cargo.toml  path to spidapter manifest
#   SPICED=$SPICEAI_REPO/target/release/spiced  path to spiced binary
#   MONGO_URI=mongodb://localhost:27017/spicebench?directConnection=true&replicaSet=rs0&tls=false
#   OUTDIR=/tmp/spicebench-mongo-<timestamp>  output directory for logs + metrics
#   VALIDATION_TIMEOUT=3600   max seconds to wait for checkpoint convergence
#   RUST_LOG=info,etl::sink::mongodb=debug,etl::sink::adbc=debug

set -uo pipefail
# Put the script in its own process group so Ctrl+C can kill all children
set -m 2>/dev/null || true

case "${1:-}" in -h|--help) sed -n '2,47p' "$0"; exit 0;; esac

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

SF="${SF:-1}"
# Bootstrap dataset shape
NUM_STEPS="${NUM_STEPS:-25}"                               # base steps (creates-only)
BOOTSTRAP_MUTATION_STEPS="${BOOTSTRAP_MUTATION_STEPS:-10}" # pure-mutation steps after the base
BOOTSTRAP_CHURN_FRACTION="${BOOTSTRAP_CHURN_FRACTION:-0.17}"
UPDATE_RATIO="${UPDATE_RATIO:-0.8}"
DELETE_RATIO="${DELETE_RATIO:-0.2}"
CHECKPOINT_INTERVAL_STEPS="${CHECKPOINT_INTERVAL_STEPS:-2}" # checkpoint cadence over mutation steps

# Parameter signature baked into the archive/checkpoint names so changing ANY
# generation knob yields a NEW path -> automatic cache miss -> regeneration. Old
# artifacts coexist (named by their params) instead of being silently reused.
#   GEN_SIG  = everything that determines the data archive (SF + generate knobs)
#   CKPT_SIG = the archive params PLUS the checkpoint cadence. Checkpoints derive
#              from the archive, so they must also invalidate when a data param
#              changes — hence CKPT_SIG is a superset of GEN_SIG.
GEN_SIG="sf${SF}-ns${NUM_STEPS}-ms${BOOTSTRAP_MUTATION_STEPS}-cf${BOOTSTRAP_CHURN_FRACTION}-ur${UPDATE_RATIO}-dr${DELETE_RATIO}"
CKPT_SIG="${GEN_SIG}-ci${CHECKPOINT_INTERVAL_STEPS}"

# DATA_ARCHIVE / CHECKPOINT_DIR are treated as a BASE location; the signature is
# always appended (for the archive, before the .tar.zst extension). Override the
# base to relocate the artifacts — the param suffix still applies, so the
# regenerate-on-param-change guarantee holds either way.
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
MONGO_URI="${MONGO_URI:-mongodb://localhost:27017/spicebench?directConnection=true&replicaSet=rs0&tls=false}"
# Atlas (mongodb+srv / *.mongodb.net) is a managed replica set — skip the local
# replica-set bring-up. Auto-detected from the URI; override with IS_ATLAS=true/false.
case "$MONGO_URI" in
  *mongodb+srv*|*mongodb.net*) IS_ATLAS="${IS_ATLAS:-true}" ;;
  *)                           IS_ATLAS="${IS_ATLAS:-false}" ;;
esac
OUTDIR="${OUTDIR:-/tmp/spicebench-mongo-$(date +%Y%m%d-%H%M%S)}"
VALIDATION_TIMEOUT="${VALIDATION_TIMEOUT:-3600}"
# Seconds spidapter waits for spiced to become ready after activate. The default
# (600) is too short for an SF1 snapshot from remote Atlas; bump it here.
READY_WAIT="${READY_WAIT:-1800}"
# Set NO_TEARDOWN=true to KEEP the per-run Atlas database after the run (for
# inspecting a stall / running the resume-token probe). Default tears down.
NO_TEARDOWN="${NO_TEARDOWN:-false}"
RUST_LOG="${RUST_LOG:-info,etl::sink::mongodb=debug,etl::sink::adbc=debug}"
# spiced log filter (passed through to the spiced child via SPICED_LOG). spiced's
# default verbosity caps non-internal crates at WARN, which hides the
# connector-mongodb resume-token / change-stream logs — surface them here.
SPICED_LOG="${SPICED_LOG:-info,connector_mongodb=debug}"
MONGO_PARALLELISM="${SPICEBENCH_MONGO_PARALLELISM:-16}"
MONGOSH="${MONGOSH:-$(command -v mongosh 2>/dev/null || echo mongosh)}"

METRICS_PORT="${METRICS_PORT:-19090}"

mkdir -p "$OUTDIR"
LOG="$OUTDIR/spicebench.log"

echo "============================================"
echo " spicebench local MongoDB run (BOOTSTRAP mode)"
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
# are already cached, the run uses the sink directly and doesn't need duckdb — and
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
  # The checkpointer reads the bootstrap layout from the archive's version.json
  # and phases identically to the run: checkpoint 0 = full base, then one
  # checkpoint per CHECKPOINT_INTERVAL_STEPS mutation steps.
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
# Step 3: Ensure MongoDB is reachable (and, for local, a replica set)
# ---------------------------------------------------------------------------
echo ""
echo "[3/4] Checking MongoDB (IS_ATLAS=$IS_ATLAS)..."
# Connectivity check against the actual target URI (works for local and Atlas).
if ! "$MONGOSH" "$MONGO_URI" --quiet --eval 'db.runCommand({ping:1})' >/dev/null 2>&1; then
  echo "ERROR: cannot connect to MongoDB at the configured MONGO_URI"
  if [ "$IS_ATLAS" = "true" ]; then
    echo "       Check the Atlas connection string, IP access list, and credentials."
  else
    echo "       Make sure MongoDB is running: brew services start mongodb-community"
  fi
  exit 1
fi

if [ "$IS_ATLAS" = "true" ]; then
  echo "      Atlas detected — managed replica set, skipping local rs bring-up."
else
  # Verify replica set is configured (required for Change Streams)
  RS_STATUS=$("$MONGOSH" --quiet --eval 'try { rs.status().ok } catch(e) { 0 }' 2>/dev/null | grep -E '^[01]$' | tail -1)
  if [ "${RS_STATUS:-0}" != "1" ]; then
    echo "      replica set not configured — initiating rs0..."
    "$MONGOSH" --quiet --eval \
      'rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]})' \
      >/dev/null 2>&1 || true
    sleep 2
    RS_STATUS=$("$MONGOSH" --quiet --eval 'try { rs.status().ok } catch(e) { 0 }' 2>/dev/null | grep -E '^[01]$' | tail -1)
    if [ "${RS_STATUS:-0}" != "1" ]; then
      echo "ERROR: failed to initiate replica set."
      echo "       Add 'replication:\\n  replSetName: rs0' to /opt/homebrew/etc/mongod.conf"
      echo "       then: brew services restart mongodb-community"
      exit 1
    fi
    echo "      replica set rs0 initiated"
  fi
fi

# Clean slate. On Atlas (connect mode) spidapter routes the run to a FRESH
# per-run database (spidapter_<short_id>) and drops it at teardown, so there's
# nothing to pre-clean here — and dropping against the URI's DB would hit the
# wrong (possibly shared/CI) database. Only pre-drop for the local instance.
if [ "$IS_ATLAS" = "true" ]; then
  echo "      Atlas — spidapter creates a fresh per-run DB; skipping pre-drop."
else
  echo "      dropping TPC-H collections (clean slate)..."
  "$MONGOSH" "$MONGO_URI" --quiet --eval '
    const tables = ["lineitem","orders","customer","part","partsupp","supplier","nation","region"];
    tables.forEach(t => { try { db[t].drop(); print("  dropped: " + t); } catch(e) {} });
  ' 2>/dev/null || true
fi
echo "      MongoDB ready"


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
SPICEBENCH_TARGET_BATCH_ROWS=640000 \
SPICEBENCH_SINK_CHUNK_ROWS=5000 \
SPICEBENCH_SINK_PARALLELISM_PER_TABLE="${SINK_PARALLELISM_PER_TABLE:-8}" \
SPICEBENCH_SINK_PARALLELISM="${SINK_PARALLELISM:-8}" \
SPICEBENCH_SINK_MAX_RECORDS_PER_SEC="${SINK_MAX_RECORDS_PER_SEC:-10000}" \
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
      --scenario mongodb-streams \
      --scenario-base-path $SCENARIO_BASE_PATH \
      --spiced-binary $SPICED" \
    --system-adapter-env "SPIDAPTER_METRICS_PORT=${METRICS_PORT}" \
    --system-adapter-env "MONGODB_URI=$MONGO_URI" \
    --system-adapter-env "SPICED_LOG=$SPICED_LOG" \
  2>&1 | tee "$LOG"
BENCH_EXIT=$?

echo ""
echo "===== Run outcome ====="
grep -iE "outcome|validation|checkpoint|passed|failed|pipeline_failure" "$LOG" \
  | tail -10

echo ""
echo "DONE  exit=$BENCH_EXIT  output=$OUTDIR"
exit "$BENCH_EXIT"

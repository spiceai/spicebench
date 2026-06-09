#!/usr/bin/env bash
#
# Run spicebench SF1 TPC-H changes against a local MongoDB replica set,
# collecting time-series metrics (spiced CPU/RSS, MongoDB row counts, ETL throughput).
#
# All state is local — no S3/MinIO required:
#   - Data is generated once and cached at DATA_ARCHIVE (default /tmp/spicebench-sf1.tar.zst)
#   - MongoDB is running
#   - spiced is launched by spidapter
#
# Usage:
#   ./scripts/run-spicebench-local-mongo.sh [--help]
#
# Configuration (environment variables, with defaults):
#   SF=1                      TPC-H scale factor
#   ETL_TYPE=changes          "events" or "changes"
#   DATA_ARCHIVE=/tmp/spicebench-sf1.tar.zst  cached data archive (generated if missing)
#   SPICEBENCH=./target/debug/spicebench      path to spicebench binary
#   SPIDAPTER=../spiceai/tools/spidapter      path to spidapter manifest
#   SPICED=../spiceai/target/debug/spiced     path to spiced binary
#   MONGO_URI=mongodb://localhost:27017/spicebench?directConnection=true&replicaSet=rs0&tls=false
#   OUTDIR=/tmp/spicebench-mongo-<timestamp>  output directory for logs + metrics
#   CHECKPOINT_DIR=/tmp/spicebench-checkpoints-sf1-changes  local checkpoint dir (auto-generated if missing)
#   VALIDATION_TIMEOUT=3600   max seconds to wait for checkpoint convergence
#   RUST_LOG=info,etl::sink::mongodb=debug,etl::sink::adbc=debug

set -uo pipefail
# Put the script in its own process group so Ctrl+C can kill all children
set -m 2>/dev/null || true

case "${1:-}" in -h|--help) sed -n '2,32p' "$0"; exit 0;; esac

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

SF="${SF:-1}"
ETL_TYPE="${ETL_TYPE:-changes}"
DATA_ARCHIVE="${DATA_ARCHIVE:-/tmp/spicebench-sf${SF}-${ETL_TYPE}.tar.zst}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-/tmp/spicebench-checkpoints-sf${SF}-${ETL_TYPE}}"
SPICEBENCH="${SPICEBENCH:-$REPO_ROOT/target/debug/spicebench}"
# Note: binary is built with --features duckdb (required for checkpoint generation)
SPIDAPTER_MANIFEST="${SPIDAPTER_MANIFEST:-/Users/viktor/workspace/spiceai/tools/spidapter/Cargo.toml}"
SPICED="${SPICED:-/Users/viktor/workspace/spicebench/spiced}"
#SPICED="${SPICED:-/Users/viktor/workspace/spice3/target/debug/spiced}"
SCENARIO_BASE_PATH="${SCENARIO_BASE_PATH:-/Users/viktor/workspace/spiceai/data/spicebench-scenarios}"
MONGO_URI="${MONGO_URI:-mongodb://localhost:27017/spicebench?directConnection=true&replicaSet=rs0&tls=false}"
OUTDIR="${OUTDIR:-/tmp/spicebench-mongo-$(date +%Y%m%d-%H%M%S)}"
VALIDATION_TIMEOUT="${VALIDATION_TIMEOUT:-3600}"
RUST_LOG="${RUST_LOG:-info,etl::sink::mongodb=debug,etl::sink::adbc=debug}"

if [ "$ETL_TYPE" = "changes" ]; then
  ETL_PREFIX="data-gen-mutable"
  GENERATE_FLAGS="--update-ratio 0.1 --delete-ratio 0.05"
else
  ETL_PREFIX="data-gen"
  GENERATE_FLAGS=""
fi

METRICS_PORT="${METRICS_PORT:-19090}"

mkdir -p "$OUTDIR"
LOG="$OUTDIR/spicebench.log"
CAYENNE_LOG="$OUTDIR/cayenne_metrics.csv"
echo "epoch,metric,dataset,value" > "$CAYENNE_LOG"

echo "============================================"
echo " spicebench local MongoDB run"
echo "  SF=$SF  ETL_TYPE=$ETL_TYPE"
echo "  archive=$DATA_ARCHIVE"
echo "  output=$OUTDIR"
echo "============================================"

# ---------------------------------------------------------------------------
# Step 1: Build spicebench if binary is missing or stale
# ---------------------------------------------------------------------------
echo ""
echo "[1/4] Building spicebench..."
RUSTC_WRAPPER="" cargo build --manifest-path "$REPO_ROOT/Cargo.toml" --features duckdb
echo "      done: $SPICEBENCH"

# ---------------------------------------------------------------------------
# Step 2: Generate data archive if not cached
# ---------------------------------------------------------------------------
echo ""
if [ -f "$DATA_ARCHIVE" ]; then
  echo "[2/4] Using cached data archive: $DATA_ARCHIVE"
else
  echo "[2/4] Generating SF${SF} $ETL_TYPE data (this may take a few minutes)..."
  RUSTC_WRAPPER="" "$SPICEBENCH" generate \
    --scale-factor "$SF" \
    --num-steps 25 \
    --dataset tpch \
    --scenario tpch \
    $GENERATE_FLAGS \
    --output-archive "$DATA_ARCHIVE"
  echo "      saved to $DATA_ARCHIVE"
fi

# Generate checkpoints if not cached — required for validation
if [ -f "$CHECKPOINT_DIR/checkpoints.json" ]; then
  echo "[2b/4] Using cached checkpoints: $CHECKPOINT_DIR"
else
  echo "[2b/4] Generating checkpoints from local archive..."
  DUCKDB_PATH="/tmp/spicebench-checkpoint-sf${SF}-${ETL_TYPE}.duckdb"
  rm -f "$DUCKDB_PATH"
  mkdir -p "$CHECKPOINT_DIR"
  if ! RUSTC_WRAPPER="" "$SPICEBENCH" checkpoint \
    --scenario tpch \
    --version "${SF}.0" \
    --duckdb-path "$DUCKDB_PATH" \
    --checkpoint-dir "$CHECKPOINT_DIR" \
    --checkpoint-interval-steps 10 \
    --etl-source-archive "$DATA_ARCHIVE"; then
    echo "ERROR: checkpoint generation failed"
    exit 1
  fi
  echo "      saved to $CHECKPOINT_DIR"
fi

# ---------------------------------------------------------------------------
# Step 3: Ensure MongoDB is running with a replica set
# ---------------------------------------------------------------------------
echo ""
echo "[3/4] Checking local MongoDB..."
if ! mongosh --quiet --eval 'db.runCommand({ping:1})' >/dev/null 2>&1; then
  echo "ERROR: cannot connect to MongoDB"
  echo "       Make sure MongoDB is running: brew services start mongodb-community"
  exit 1
fi

# Verify replica set is configured (required for Change Streams)
RS_STATUS=$(mongosh --quiet --eval 'try { rs.status().ok } catch(e) { 0 }' 2>/dev/null | grep -E '^[01]$' | tail -1)
if [ "${RS_STATUS:-0}" != "1" ]; then
  echo "      replica set not configured — initiating rs0..."
  mongosh --quiet --eval \
    'rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]})' \
    >/dev/null 2>&1 || true
  sleep 2
  # Verify
  RS_STATUS=$(mongosh --quiet --eval 'try { rs.status().ok } catch(e) { 0 }' 2>/dev/null | grep -E '^[01]$' | tail -1)
  if [ "${RS_STATUS:-0}" != "1" ]; then
    echo "ERROR: failed to initiate replica set."
    echo "       Add 'replication:\\n  replSetName: rs0' to /opt/homebrew/etc/mongod.conf"
    echo "       then: brew services restart mongodb-community"
    exit 1
  fi
  echo "      replica set rs0 initiated"
fi

# Drop all TPC-H collections for a clean slate
echo "      dropping TPC-H collections..."
mongosh "$MONGO_URI" --quiet --eval '
  const tables = ["lineitem","orders","customer","part","partsupp","supplier","nation","region"];
  tables.forEach(t => { db[t].drop(); print("  dropped: " + t); });
' 2>/dev/null || true
echo "      MongoDB ready"


# ---------------------------------------------------------------------------
# Metric collection: Cayenne/CDC metrics from spiced Prometheus endpoint every 5s
# ---------------------------------------------------------------------------
(
  METRICS_ENDPOINT="http://localhost:${METRICS_PORT}/metrics"
  LAST_METRICS_FILE="${CAYENNE_LOG%.csv}_last_raw.txt"
  while true; do
    body=$(curl -s --max-time 3 "$METRICS_ENDPOINT" 2>/dev/null)
    if [ -n "$body" ]; then
      # Save last successful full scrape for the final report (spiced may be
      # killed before the summary runs, so we can't scrape again then)
      echo "$body" > "$LAST_METRICS_FILE"
      ts=$(date +%s)
      echo "$body" | grep -E "^dataset_acceleration_(ingestion_lag_ms|refresh_lag_ms|refresh_processed_rows|refresh_errors)" \
        | while IFS= read -r line; do
          metric=$(echo "$line" | sed 's/{.*//')
          dataset=$(echo "$line" | grep -o 'dataset="[^"]*"' | cut -d'"' -f2)
          value=$(echo "$line" | awk '{print $NF}')
          [ -n "$metric" ] && [ -n "$value" ] && \
            echo "${ts},${metric},${dataset:-unknown},${value}" >> "$CAYENNE_LOG"
        done
    fi
    sleep 5
  done
) &
CAYENNE_MONITOR_PID=$!

# Kill background monitors and the spicebench process on Ctrl+C / SIGTERM
cleanup() {
  echo ""
  echo "Interrupted — stopping background monitors..."
  kill "$CAYENNE_MONITOR_PID" 2>/dev/null || true
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

RUSTC_WRAPPER="" \
SPICEBENCH_TARGET_BATCH_ROWS=100000 \
RUST_LOG="$RUST_LOG" \
  "$SPICEBENCH" run \
    --scenario tpch \
    --scale-factor "$SF" \
    --etl-sink adbc \
    --etl-source-archive "$DATA_ARCHIVE" \
    --validate-results \
    --checkpoint-local-dir "$CHECKPOINT_DIR" \
    --checkpoint-validation-timeout "$VALIDATION_TIMEOUT" \
    --system-adapter-stdio-cmd cargo \
    --system-adapter-stdio-args "run --manifest-path $SPIDAPTER_MANIFEST -- stdio \
      --scenario mongodb-streams \
      --scenario-base-path $SCENARIO_BASE_PATH \
      --spiced-binary $SPICED" \
    --system-adapter-env "SPIDAPTER_METRICS_PORT=${METRICS_PORT}" \
    --system-adapter-env "MONGODB_URI=$MONGO_URI" \
  2>&1 | tee "$LOG"
BENCH_EXIT=$?

# ---------------------------------------------------------------------------
# Final metrics snapshot before stopping collectors
# ---------------------------------------------------------------------------
kill "$CAYENNE_MONITOR_PID" 2>/dev/null || true

LAST_METRICS_FILE="${CAYENNE_LOG%.csv}_last_raw.txt"
FINAL_METRICS=""
if [ -f "$LAST_METRICS_FILE" ]; then
  FINAL_METRICS=$(cat "$LAST_METRICS_FILE")
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

# Cayenne write-phase report: avg = sum/count per phase, sorted by avg desc
echo ""
echo "===== Cayenne write-phase report ====="
if [ -n "$FINAL_METRICS" ]; then
  _METRICS_TMP=$(mktemp)
  echo "$FINAL_METRICS" > "$_METRICS_TMP"
  python3 <<PYEOF
import re

data = open("$_METRICS_TMP").read()
counts = {}
sums   = {}

for line in data.splitlines():
    if line.startswith('#'): continue
    m = re.match(r'cayenne_write_phase_duration_ms_(count|sum)\{[^}]*phase="([^"]+)"[^}]*\}\s+([\d.e+\-]+)', line)
    if m:
        kind, phase, val = m.group(1), m.group(2), float(m.group(3))
        if kind == 'count': counts[phase] = counts.get(phase, 0) + val
        else:               sums[phase]   = sums.get(phase, 0)   + val

phases = sorted(counts.keys(), key=lambda p: sums.get(p,0)/counts[p] if counts[p] else 0, reverse=True)

print(f"{'Phase':<40} {'Count':>8}  {'Avg (ms)':>10}  {'Sum (ms)':>12}")
print("-" * 76)
for p in phases:
    c = counts[p]
    s = sums.get(p, 0)
    avg = s / c if c else 0
    print(f"{p:<40} {c:>8.0f}  {avg:>10.2f}  {s:>12.0f}")
PYEOF
  rm -f "$_METRICS_TMP"
else
  echo "  (metrics endpoint not reachable — was SPIDAPTER_METRICS_PORT set?)"
fi

echo ""
echo "===== CDC ingestion lag (from scrape log) ====="
awk -F, 'NR>1 && $2=="dataset_acceleration_ingestion_lag_ms" {
  if($4>max[$3]) max[$3]=$4
} END {
  for(d in max) printf "  %-15s peak_lag=%.0fms\n", d, max[d]
}' "$CAYENNE_LOG" 2>/dev/null | sort || echo "  (no data)"

echo ""
echo "===== Run outcome ====="
grep -iE "outcome|validation|checkpoint|passed|failed|pipeline_failure" "$LOG" \
  | tail -10

echo ""
echo "DONE  exit=$BENCH_EXIT  output=$OUTDIR"
exit "$BENCH_EXIT"

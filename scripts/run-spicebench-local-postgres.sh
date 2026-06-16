#!/usr/bin/env bash
#
# Run spicebench SF1 TPC-H changes against a local PostgreSQL (WAL CDC),
# collecting time-series metrics (spiced CPU/RSS, PostgreSQL stats, ETL throughput).
# Mirrors run-spicebench-local-mongo.sh for direct performance comparison — the
# only difference is the source/sink is PostgreSQL WAL instead of MongoDB streams.
#
# All state is local — no S3/MinIO required:
#   - Data is generated once and cached at DATA_ARCHIVE (default ./data/spicebench-sf1-changes.tar.zst)
#   - PostgreSQL is running (wal_level=logical)
#   - spiced is launched by spidapter
#
# Usage:
#   ./scripts/run-spicebench-local-postgres.sh [--help]
#
# Configuration (environment variables, with defaults):
#   SF=1                        TPC-H scale factor
#   ETL_TYPE=changes            "events" or "changes"
#   DATA_ARCHIVE=./data/spicebench-sf1-changes.tar.zst  cached data archive (generated if missing)
#   SPICEBENCH=./target/release/spicebench      path to spicebench binary
#   SPIDAPTER_MANIFEST=../spiceai/tools/spidapter/Cargo.toml
#   SPICED=../spiceai/target/debug/spiced       path to spiced binary
#   SCENARIO_BASE_PATH=../spiceai/data/spicebench-scenarios
#   PG_HOST=localhost  PG_PORT=5432  PG_USER=viktor  PG_PASSWORD=  PG_DATABASE=spicebench
#   OUTDIR=/tmp/spicebench-postgres-<timestamp>  output directory for logs + metrics
#   CHECKPOINT_DIR=./data/spicebench-checkpoints-sf1-changes  local checkpoint dir (auto-generated if missing)
#   VALIDATION_TIMEOUT=3600   max seconds to wait for checkpoint convergence
#   RUST_LOG=info,etl::sink::adbc=debug

set -uo pipefail
# Put the script in its own process group so Ctrl+C can kill all children
set -m 2>/dev/null || true

case "${1:-}" in -h|--help) sed -n '2,32p' "$0"; exit 0;; esac

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

SF="${SF:-1}"
ETL_TYPE="${ETL_TYPE:-changes}"
DATA_ARCHIVE="${DATA_ARCHIVE:-$REPO_ROOT/data/spicebench-sf${SF}-${ETL_TYPE}.tar.zst}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-$REPO_ROOT/data/spicebench-checkpoints-sf${SF}-${ETL_TYPE}}"
SPICEBENCH="${SPICEBENCH:-$REPO_ROOT/target/release/spicebench}"
# Note: binary is built with --features duckdb (required for checkpoint generation)
SPIDAPTER_MANIFEST="${SPIDAPTER_MANIFEST:-/Users/viktor/workspace/spiceai/tools/spidapter/Cargo.toml}"
SPICED="${SPICED:-/Users/viktor/workspace/spiceai/target/release/spiced}"
# Local runs use the local-compute scenarios (compute: local). The in-repo
# scenarios/ dir holds the CI variants (compute: scp), so don't default to it here.
SCENARIO_BASE_PATH="${SCENARIO_BASE_PATH:-/Users/viktor/workspace/spiceai/data/spicebench-scenarios}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-viktor}"
PG_PASSWORD="${PG_PASSWORD:-}"
PG_DATABASE="${PG_DATABASE:-spicebench}"
OUTDIR="${OUTDIR:-/tmp/spicebench-postgres-$(date +%Y%m%d-%H%M%S)}"
VALIDATION_TIMEOUT="${VALIDATION_TIMEOUT:-3600}"
RUST_LOG="${RUST_LOG:-info,etl::sink::adbc=debug}"
PSQL="${PSQL:-$(command -v psql 2>/dev/null || echo /opt/homebrew/bin/psql)}"

if [ -n "${PG_PASSWORD:-}" ]; then
  PG_DSN="host=${PG_HOST} port=${PG_PORT} user=${PG_USER} password=${PG_PASSWORD} dbname=${PG_DATABASE} sslmode=disable"
else
  PG_DSN="host=${PG_HOST} port=${PG_PORT} user=${PG_USER} dbname=${PG_DATABASE} sslmode=disable"
fi

if [ "$ETL_TYPE" = "changes" ]; then
  GENERATE_FLAGS="--update-ratio 0.1 --delete-ratio 0.05"
  SPICEBENCH_ADBC_UPDATE_STRATEGY="staging_table"
else
  GENERATE_FLAGS=""
  SPICEBENCH_ADBC_UPDATE_STRATEGY="bulk_ingest_upsert"
fi

METRICS_PORT="${METRICS_PORT:-19090}"

mkdir -p "$OUTDIR"
LOG="$OUTDIR/spicebench.log"
CAYENNE_LOG="$OUTDIR/cayenne_metrics.csv"
echo "epoch,metric,dataset,value" > "$CAYENNE_LOG"

echo "============================================"
echo " spicebench local PostgreSQL WAL run"
echo "  SF=$SF  ETL_TYPE=$ETL_TYPE"
echo "  archive=$DATA_ARCHIVE"
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

# ---------------------------------------------------------------------------
# Metric collection: PostgreSQL stats every 5s (analog of MongoDB serverStatus)
# ---------------------------------------------------------------------------
PG_METRICS_LOG="$OUTDIR/postgres_metrics.csv"
echo "ts,xact_commit,tup_inserted,tup_updated,tup_deleted,active_conns,repl_lag_bytes,wal_bytes" \
  > "$PG_METRICS_LOG"
(
  _p_xact=-1; _p_ins=-1; _p_upd=-1; _p_del=-1; _p_wal=-1
  while true; do
    # One row of: xact_commit, tup_inserted, tup_updated, tup_deleted (cumulative
    # counters), active connections + max replication-slot lag bytes (gauges),
    # total WAL bytes generated (cumulative).
    row=$("$PSQL" "$PG_DSN" -tA -F',' -c "
      SELECT
        COALESCE(d.xact_commit, 0),
        COALESCE(d.tup_inserted, 0),
        COALESCE(d.tup_updated, 0),
        COALESCE(d.tup_deleted, 0),
        (SELECT count(*) FROM pg_stat_activity WHERE state = 'active'),
        COALESCE((SELECT max(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))::bigint
                  FROM pg_replication_slots WHERE restart_lsn IS NOT NULL), 0),
        pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint
      FROM pg_stat_database d
      WHERE d.datname = '${PG_DATABASE}';
    " 2>/dev/null | tail -1)

    if [ -n "$row" ]; then
      ts=$(date +%s)
      _c_xact=$(echo "$row" | cut -d, -f1)
      _c_ins=$(echo "$row" | cut -d, -f2)
      _c_upd=$(echo "$row" | cut -d, -f3)
      _c_del=$(echo "$row" | cut -d, -f4)
      _conns=$(echo "$row" | cut -d, -f5)
      _lag=$(echo "$row" | cut -d, -f6)
      _c_wal=$(echo "$row" | cut -d, -f7)
      if [ "$_p_xact" = "-1" ]; then
        # First sample — use as baseline for cumulative counters, don't record
        _p_xact=$_c_xact; _p_ins=$_c_ins; _p_upd=$_c_upd; _p_del=$_c_del; _p_wal=$_c_wal
      else
        echo "${ts},$(( _c_xact - _p_xact )),$(( _c_ins - _p_ins )),$(( _c_upd - _p_upd )),$(( _c_del - _p_del )),${_conns},${_lag},$(( _c_wal - _p_wal ))" \
          >> "$PG_METRICS_LOG"
        _p_xact=$_c_xact; _p_ins=$_c_ins; _p_upd=$_c_upd; _p_del=$_c_del; _p_wal=$_c_wal
      fi
    fi
    sleep 5
  done
) &
PG_MONITOR_PID=$!

stop_monitors() {
  kill "$CAYENNE_MONITOR_PID" 2>/dev/null || true
  kill "$PG_MONITOR_PID" 2>/dev/null || true
}

cleanup() {
  echo ""
  echo "Interrupted — stopping background monitors..."
  stop_monitors
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
SPICEBENCH_ADBC_UPDATE_STRATEGY="$SPICEBENCH_ADBC_UPDATE_STRATEGY" \
SPICEBENCH_ADBC_DELETE_BATCH_SIZE=5000 \
SPICEBENCH_ADBC_REUSE_BULK_INGEST_STREAMS=false \
SPICEBENCH_ADBC_ANALYZE_STAGING_BEFORE_MERGE=true \
SPICEBENCH_SINK_PARALLELISM_PER_TABLE=8 \
SPICEBENCH_SINK_CHUNK_ROWS=40000 \
SPICEBENCH_SINK_PARALLELISM=8 \
RUST_LOG="$RUST_LOG" \
  "$SPICEBENCH" run \
    --scenario tpch \
    --scale-factor "$SF" \
    --etl-sink adbc \
    --etl-source-archive "$DATA_ARCHIVE" \
    --validate-results \
    --scrape-sut-metrics \
    --checkpoint-local-dir "$CHECKPOINT_DIR" \
    --checkpoint-validation-timeout "$VALIDATION_TIMEOUT" \
    --system-adapter-stdio-cmd cargo \
    --system-adapter-stdio-args "run --manifest-path $SPIDAPTER_MANIFEST -- stdio \
      --scenario postgres-wal \
      --scenario-base-path $SCENARIO_BASE_PATH \
      --spiced-binary $SPICED" \
    --system-adapter-env "SPIDAPTER_METRICS_PORT=${METRICS_PORT}" \
    --system-adapter-env "PG_HOST=${PG_HOST}" \
    --system-adapter-env "PG_PORT=${PG_PORT}" \
    --system-adapter-env "PG_USER=${PG_USER}" \
    --system-adapter-env "PG_PASSWORD=${PG_PASSWORD:-none}" \
    --system-adapter-env "PG_DATABASE=${PG_DATABASE}" \
  2>&1 | tee "$LOG"
BENCH_EXIT=$?

stop_monitors

# ---------------------------------------------------------------------------
# Final metrics snapshot before stopping collectors
# ---------------------------------------------------------------------------
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
echo "===== PostgreSQL stats summary ====="
if [ -f "$PG_METRICS_LOG" ] && [ "$(wc -l < "$PG_METRICS_LOG")" -gt 1 ]; then
  python3 << PYEOF
import csv, sys

rows = []
with open("$PG_METRICS_LOG") as f:
    for r in csv.DictReader(f):
        try:
            rows.append({k: int(v) for k, v in r.items() if k != 'ts'} | {'ts': int(r['ts'])})
        except ValueError:
            pass

if not rows:
    print("  (no data)")
    sys.exit(0)

def pct(vals, p):
    s = sorted(vals)
    return s[int(len(s) * p / 100)] if s else 0

def fmt(label, vals, unit=""):
    if not vals: return
    print(f"  {label:<35} avg={sum(vals)/len(vals):>10.1f}{unit}  p99={pct(vals,99):>10.1f}{unit}  max={max(vals):>10.1f}{unit}")

xact  = [r['xact_commit']   for r in rows]
ins   = [r['tup_inserted']  for r in rows]
upd   = [r['tup_updated']   for r in rows]
dele  = [r['tup_deleted']   for r in rows]
conns = [r['active_conns']  for r in rows]
lag   = [r['repl_lag_bytes'] / 1048576 for r in rows]   # bytes -> MB
wal   = [r['wal_bytes'] / 1048576 for r in rows]        # bytes -> MB

print(f"  {'Metric':<35} {'avg':>12}  {'p99':>12}  {'max':>12}")
print("  " + "-" * 64)
fmt("commits/5s",                 xact)
fmt("tup_inserted/5s",            ins)
fmt("tup_updated/5s",             upd)
fmt("tup_deleted/5s",             dele)
fmt("active_connections",         conns)
fmt("repl_slot_lag",              lag, "MB")
fmt("wal_generated/5s",           wal, "MB")
PYEOF
else
  echo "  (no data — psql not found at: $PSQL)"
fi

echo ""
echo "===== CDC bottleneck analysis (spice vs source) ====="
if [ -f "$LAST_METRICS_FILE" ]; then
  python3 << PYEOF
import re, sys

data = open("$LAST_METRICS_FILE").read()

def get_sum(metric, dataset=None):
    if dataset:
        pat = rf'^{re.escape(metric)}_sum\{{[^}}]*dataset="{re.escape(dataset)}"[^}}]*\}}\s+([\d.e+\-]+)'
    else:
        pat = rf'^{re.escape(metric)}_sum\{{[^}}]*\}}\s+([\d.e+\-]+)'
    vals = [float(m) for m in re.findall(pat, data, re.MULTILINE)]
    return sum(vals)

# Find all datasets that have CDC metrics
datasets = sorted(set(re.findall(
    r'dataset_acceleration_cdc_source_recv_wait_ms_sum\{[^}]*dataset="([^"]+)"',
    data)))

if not datasets:
    print("  (no CDC metrics found — spiced not running or metrics endpoint unreachable)")
    sys.exit(0)

print(f"  {'dataset':<15} {'recv_wait_s':>12} {'apply_s':>10} {'recv_%':>9}  verdict")
print("  " + "-" * 70)

for ds in datasets:
    recv_sum = get_sum("dataset_acceleration_cdc_source_recv_wait_ms", ds) / 1000
    apply_sum = get_sum("dataset_acceleration_cdc_apply_burst_duration_ms", ds) / 1000
    total = recv_sum + apply_sum
    if total < 0.001:
        continue
    recv_pct = 100 * recv_sum / total
    if recv_pct > 70:
        verdict = "SOURCE CDC bottleneck (spice idle, waiting for WAL events)"
    elif recv_pct < 30:
        verdict = "SPICE bottleneck (cayenne apply can't keep up)"
    else:
        verdict = "balanced"
    print(f"  {ds:<15} {recv_sum:>12.1f} {apply_sum:>10.1f} {recv_pct:>8.1f}%  {verdict}")

print()

# Overall ingestion lag
lag_datasets = sorted(set(re.findall(
    r'dataset_acceleration_ingestion_lag_ms\{[^}]*dataset="([^"]+)"', data)))
if lag_datasets:
    print("  Current ingestion lag per dataset:")
    for ds in lag_datasets:
        pat = rf'dataset_acceleration_ingestion_lag_ms\{{[^}}]*dataset="{re.escape(ds)}"[^}}]*\}}\s+([\d.e+\-]+)'
        vals = [float(m) for m in re.findall(pat, data, re.MULTILINE)]
        if vals:
            print(f"    {ds:<15} lag={max(vals):.0f}ms")
PYEOF
  echo "  LAST_METRICS_FILE=$LAST_METRICS_FILE"
else
  echo "  (no metrics file — was SPIDAPTER_METRICS_PORT set?)"
fi

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

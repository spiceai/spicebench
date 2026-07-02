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
#   DATA_ARCHIVE=./data/spicebench-sf<SF>-bootstrap.tar.zst  cached data archive (generated if missing)
#   CHECKPOINT_DIR=./data/spicebench-checkpoints-sf<SF>-bootstrap  local checkpoint dir (auto-generated if missing)
#   SPICEBENCH=./target/release/spicebench    path to spicebench binary
#   SPIDAPTER_MANIFEST=../spiceai/tools/spidapter/Cargo.toml  path to spidapter manifest
#   SPICED=../spiceai/target/debug/spiced     path to spiced binary
#   MONGO_URI=mongodb://localhost:27017/spicebench?directConnection=true&replicaSet=rs0&tls=false
#   OUTDIR=/tmp/spicebench-mongo-<timestamp>  output directory for logs + metrics
#   VALIDATION_TIMEOUT=3600   max seconds to wait for checkpoint convergence
#   RUST_LOG=info,etl::sink::mongodb=debug,etl::sink::adbc=debug

set -uo pipefail
# Put the script in its own process group so Ctrl+C can kill all children
set -m 2>/dev/null || true

case "${1:-}" in -h|--help) sed -n '2,46p' "$0"; exit 0;; esac

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
DATA_ARCHIVE="${DATA_ARCHIVE:-$REPO_ROOT/data/spicebench-sf${SF}-bootstrap.tar.zst}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-$REPO_ROOT/data/spicebench-checkpoints-sf${SF}-bootstrap}"
SPICEBENCH="${SPICEBENCH:-$REPO_ROOT/target/release/spicebench}"
# Note: binary is built with --features duckdb (required for checkpoint generation)
SPIDAPTER_MANIFEST="${SPIDAPTER_MANIFEST:-/Users/viktor/workspace/spiceai/tools/spidapter/Cargo.toml}"
SPICED="${SPICED:-/Users/viktor/workspace/spiceai/target/debug/spiced}"
#SPICED="${SPICED:-/Users/viktor/workspace/spiceai/target/debug/spiced_broken}"
# Local runs use the local-compute scenarios (compute: local) under
# spiceai/data/spicebench-scenarios. The CI variants (compute: scp) live in the
# spidapter image at tools/spidapter/scenarios, so don't default to those here.
SCENARIO_BASE_PATH="${SCENARIO_BASE_PATH:-/Users/viktor/workspace/spiceai/data/spicebench-scenarios}"
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
MONGOSH="${MONGOSH:-$(command -v mongosh 2>/dev/null || echo /opt/homebrew/bin/mongosh)}"

METRICS_PORT="${METRICS_PORT:-19090}"

mkdir -p "$OUTDIR"
LOG="$OUTDIR/spicebench.log"
CAYENNE_LOG="$OUTDIR/cayenne_metrics.csv"
echo "epoch,metric,dataset,value" > "$CAYENNE_LOG"

echo "============================================"
echo " spicebench local MongoDB run (BOOTSTRAP mode)"
echo "  SF=$SF  base_steps=$NUM_STEPS  mutation_steps=$BOOTSTRAP_MUTATION_STEPS"
echo "  churn=$BOOTSTRAP_CHURN_FRACTION  update/delete=$UPDATE_RATIO/$DELETE_RATIO"
echo "  checkpoint_interval=$CHECKPOINT_INTERVAL_STEPS"
echo "  archive=$DATA_ARCHIVE"
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
  DUCKDB_PATH="/tmp/spicebench-checkpoint-sf${SF}-bootstrap.duckdb"
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
  # Drop stale per-run databases left behind by previous (especially interrupted)
  # runs. spidapter routes each local run to its own `spidapter_<short_id>` database
  # and only drops it at a clean teardown; a Ctrl+C / crash leaks it. Left to
  # accumulate, these compete for the WiredTiger cache and thrash the mutation
  # write path — measured ~24x slowdown (55 -> 1315 _id-upserts/sec) once the
  # combined working set exceeds the cache. Reap them here so each run starts with
  # only its own dataset resident.
  echo "      dropping stale per-run databases (spidapter_*)..."
  "$MONGOSH" "$MONGO_URI" --quiet --eval '
    db.getSiblingDB("admin").adminCommand({ listDatabases: 1, nameOnly: true }).databases
      .map(d => d.name)
      .filter(n => /^spidapter_/.test(n))
      .forEach(n => { try { db.getSiblingDB(n).dropDatabase(); print("  dropped db: " + n); } catch(e) {} });
  ' 2>/dev/null || true
fi
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

# ---------------------------------------------------------------------------
# Metric collection: MongoDB serverStatus metrics every 5s
# ---------------------------------------------------------------------------
MONGO_METRICS_LOG="$OUTDIR/mongo_metrics.csv"
echo "ts,op_insert,op_update,op_delete,queue_writers,active_writers,wt_write_tickets_used,wt_write_tickets_avail,wt_write_queue_len,cache_mb,cache_dirty_mb,evict_app_threads,connections_active,flow_lagged" \
  > "$MONGO_METRICS_LOG"
(
  _prev_insert=-1; _prev_update=-1; _prev_delete=-1; _prev_evict=-1
  while true; do
    row=$("$MONGOSH" --quiet "$MONGO_URI" --eval '
      const s = db.serverStatus();
      const wt = s.wiredTiger;
      const c = wt.cache;
      const wq = s.queues.execution.write;
      print([
        s.opcounters.insert,
        s.opcounters.update,
        s.opcounters.delete,
        s.globalLock.currentQueue.writers,
        s.globalLock.activeClients.writers,
        wq.out,
        wq.available,
        wq.normalPriority.queueLength,
        Math.round(c["bytes currently in the cache"]/1048576),
        Math.round(c["tracked dirty bytes in the cache"]/1048576),
        c["page evict attempts by application threads"],
        s.connections.active,
        s.flowControl.isLagged ? 1 : 0
      ].join(","));
    ' 2>/dev/null | tail -1)

    if [ -n "$row" ]; then
      ts=$(date +%s)
      # Cumulative counters: insert(1), update(2), delete(3), evict(11)
      _cur_insert=$(echo "$row" | cut -d, -f1)
      _cur_update=$(echo "$row" | cut -d, -f2)
      _cur_delete=$(echo "$row" | cut -d, -f3)
      _cur_evict=$(echo  "$row" | cut -d, -f11)
      # Non-cumulative fields (4-10, 12-13)
      _mid=$(echo "$row" | cut -d, -f4-10)
      _tail=$(echo "$row" | cut -d, -f12-13)
      if [ "$_prev_insert" = "-1" ]; then
        # First sample — use as baseline, don't record
        _prev_insert=$_cur_insert; _prev_update=$_cur_update
        _prev_delete=$_cur_delete; _prev_evict=$_cur_evict
      else
        _delta_insert=$(( _cur_insert - _prev_insert ))
        _delta_update=$(( _cur_update - _prev_update ))
        _delta_delete=$(( _cur_delete - _prev_delete ))
        _delta_evict=$(( _cur_evict - _prev_evict ))
        _prev_insert=$_cur_insert; _prev_update=$_cur_update
        _prev_delete=$_cur_delete; _prev_evict=$_cur_evict
        echo "${ts},${_delta_insert},${_delta_update},${_delta_delete},${_mid},${_delta_evict},${_tail}" >> "$MONGO_METRICS_LOG"
      fi
    fi
    sleep 5
  done
) &
MONGO_MONITOR_PID=$!

# Kill any leftover cdc-latency-probe processes from previous runs
pkill -f "cdc-latency-probe.py" 2>/dev/null || true

# ---------------------------------------------------------------------------
# CDC row-count monitor (cdc-latency-probe.py)
# ---------------------------------------------------------------------------
CDC_PROBE_SCRIPT="$REPO_ROOT/scripts/cdc-latency-probe.py"
CDC_PROBE_LOG="$OUTDIR/cdc_probe.log"
(
  PYTHONUNBUFFERED=1 MONGODB_URI="$MONGO_URI" uv run "$CDC_PROBE_SCRIPT" 2>&1
) >> "$CDC_PROBE_LOG" &
CDC_PROBE_PID=$!
echo "      cdc-latency-probe started (pid=$CDC_PROBE_PID, log=$CDC_PROBE_LOG)"

stop_monitors() {
  kill "$CAYENNE_MONITOR_PID" 2>/dev/null || true
  kill "$MONGO_MONITOR_PID" 2>/dev/null || true
  kill "$CDC_PROBE_PID" 2>/dev/null || true
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

stop_monitors

# ---------------------------------------------------------------------------
# Final metrics snapshot before stopping collectors
# ---------------------------------------------------------------------------
kill "$CAYENNE_MONITOR_PID" 2>/dev/null || true
kill "$MONGO_MONITOR_PID" 2>/dev/null || true

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
echo "===== MongoDB serverStatus summary ====="
if [ -f "$MONGO_METRICS_LOG" ] && [ "$(wc -l < "$MONGO_METRICS_LOG")" -gt 1 ]; then
  python3 << PYEOF
import csv, sys

rows = []
with open("$MONGO_METRICS_LOG") as f:
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
    print(f"  {label:<35} avg={sum(vals)/len(vals):>8.1f}{unit}  p99={pct(vals,99):>8.1f}{unit}  max={max(vals):>8.1f}{unit}")

op_i  = [r['op_insert']  for r in rows]
op_u  = [r['op_update']  for r in rows]
op_d  = [r['op_delete']  for r in rows]
qw    = [r['queue_writers'] for r in rows]
aw    = [r['active_writers'] for r in rows]
wtu   = [r['wt_write_tickets_used'] for r in rows]
wta   = [r['wt_write_tickets_avail'] for r in rows]
wtq   = [r['wt_write_queue_len'] for r in rows]
cache = [r['cache_mb'] for r in rows]
dirty = [r['cache_dirty_mb'] for r in rows]
evict = [r['evict_app_threads'] for r in rows]
conns = [r['connections_active'] for r in rows]
lagged_pct = 100 * sum(1 for r in rows if r['flow_lagged']) / len(rows)

print(f"  {'Metric':<35} {'avg':>10}  {'p99':>10}  {'max':>10}")
print("  " + "-" * 60)
fmt("ops/5s insert",               op_i)
fmt("ops/5s update",               op_u)
fmt("ops/5s delete",               op_d)
fmt("queue_writers (globalLock)",  qw)
fmt("active_writers (globalLock)", aw)
fmt("wt_write_tickets_used",       wtu)
fmt("wt_write_tickets_avail",      wta)
fmt("wt_write_queue_len",          wtq)
fmt("cache_mb",                    cache,  "MB")
fmt("cache_dirty_mb",              dirty,  "MB")
fmt("evict_app_threads (per 5s)",  evict)
fmt("connections_active",          conns)
print(f"  {'flow_control_lagged':<35} {lagged_pct:.1f}% of scrapes")
PYEOF
else
  echo "  (no data — mongosh not found at: $MONGOSH)"
fi

echo ""
echo "===== CDC bottleneck analysis (spice vs mongo) ====="
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

def get_count(metric, dataset=None):
    if dataset:
        pat = rf'^{re.escape(metric)}_count\{{[^}}]*dataset="{re.escape(dataset)}"[^}}]*\}}\s+([\d.e+\-]+)'
    else:
        pat = rf'^{re.escape(metric)}_count\{{[^}}]*\}}\s+([\d.e+\-]+)'
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
        verdict = "MONGO CDC bottleneck (spice idle, waiting for events)"
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
echo "DONE  exit=$BENCH_EXIT  output=$OUTDIR"
exit "$BENCH_EXIT"

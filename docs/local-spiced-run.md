# Local Spice Runtime Run (spiced + spidapter + MinIO)

This guide shows how to set up a **full local SpiceBench run from scratch** against a locally-built Spice runtime cluster (scheduler + executors) launched by `spidapter`.

It covers:

1. building `spicebench`, `spiced`, and `spidapter`
2. starting MinIO locally
3. generating the mutable SF0.1 dataset locally
4. generating checkpoints locally
5. running the benchmark against local `spiced`
6. verifying that both checkpoints converge and the benchmark finishes successfully

## Scope and limitations

- This uses the `local` `spidapter` backend, so reads and writes go directly to a local scheduler / executor cluster.
- It reproduces the local Spice runtime path only. It does **not** include the Spice Cloud API / Flight proxy path.
- For the most comparable mutable validation run, use:
  - scale factor `0.1`
  - ETL prefix `data-gen-mutable`
  - `4` executors
  - `--concurrency 1`
  - `--validate-results`
- Mutable data generation is **not deterministic**. Two generations of SF0.1 will produce slightly different row counts and checkpoint artifacts.

## Prerequisites

- Rust
- Docker
- AWS CLI
- `spicebench` repo at `~/code/spiceai/spicebench`
- `spiceai` repo at `~/code/spiceai/spiceai`

## 1. Build the binaries

Build `spicebench` with the `duckdb` feature so checkpoint generation is available:

```bash
cd ~/code/spiceai/spicebench
cargo build --release -p spicebench --features duckdb
```

Build `spiced` and `spidapter`:

```bash
cd ~/code/spiceai/spiceai
cargo build --release -p spiced -p spidapter
```

Install the freshly-built `spiced` where `spidapter` expects to find it:

```bash
mkdir -p ~/.spice/bin
cp ~/code/spiceai/spiceai/target/release/spiced ~/.spice/bin/spiced
```

## 2. Start MinIO

If you already have a `spicebench-minio` container, start it. Otherwise create it:

```bash
docker start spicebench-minio >/dev/null 2>&1 || \
  docker run -d --name spicebench-minio \
    -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"
```

Create the local S3 bucket used by SpiceBench:

```bash
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://spicebench || true
```

You can verify MinIO from:

- API: `http://127.0.0.1:9000`
- Console: `http://127.0.0.1:9001`
- login: `minioadmin` / `minioadmin`

## 3. Generate the mutable SF0.1 dataset locally

Generate a mutable TPC-H dataset into MinIO under:

```text
s3://spicebench/data-gen-mutable/tpch/0.1/
```

Run:

```bash
cd ~/code/spiceai/spicebench

AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
./target/release/spicebench generate \
  --scale-factor 0.1 \
  --bucket spicebench \
  --prefix data-gen-mutable \
  --region us-east-1 \
  --endpoint http://127.0.0.1:9000 \
  --num-steps 20 \
  --update-ratio 0.1 \
  --delete-ratio 0.05
```

This writes the generated archive to:

```text
s3://spicebench/data-gen-mutable/tpch/0.1/data.tar.zst
```

## 4. Generate checkpoints locally

Generate checkpoints for that dataset so `--validate-results` has expected outputs to compare against.

```bash
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
./target/release/spicebench checkpoint \
  --scenario tpch \
  --version 0.1 \
  --bucket spicebench \
  --prefix data-gen-mutable \
  --endpoint http://127.0.0.1:9000 \
  --region us-east-1 \
  --duckdb-path /tmp/spicebench-sf01-checkpoints.duckdb \
  --checkpoint-interval-steps 10 \
  --checkpoint-dir /tmp/spicebench-sf01-checkpoints
```

This uploads:

```text
s3://spicebench/data-gen-mutable/tpch/0.1/checkpoints.json
s3://spicebench/data-gen-mutable/tpch/0.1/checkpoints/...
```

Verify the local prefix:

```bash
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
aws --endpoint-url http://127.0.0.1:9000 s3 ls \
  s3://spicebench/data-gen-mutable/tpch/0.1/ \
  --recursive --summarize
```

You should see:

- `data.tar.zst`
- `checkpoints.json`
- files under `checkpoints/0/` and `checkpoints/1/`

## 5. Run the local benchmark

Use a file-backed scheduler state location so the local run does not require AWS credentials.

```bash
RUN_LOG=/tmp/spicebench-sf01-local-$(date +%s).log
SCHEDULER_STATE_LOCATION=file:///tmp/spicebench-scheduler-state-sf01-$(date +%s)

cd ~/code/spiceai/spicebench

AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
SPIDAPTER_BACKEND=local \
SPIDAPTER_NUM_EXECUTORS=4 \
SPICEBENCH_ADBC_UPDATE_STRATEGY=bulk_ingest_upsert \
SPICEBENCH_ADBC_DELETE_BATCH_SIZE=50000 \
SPICEBENCH_ADBC_REUSE_BULK_INGEST_STREAMS=false \
./target/release/spicebench run \
  --scenario tpch \
  --scale-factor 0.1 \
  --etl-bucket spicebench \
  --etl-prefix data-gen-mutable \
  --etl-region us-east-1 \
  --etl-endpoint http://127.0.0.1:9000 \
  --system-adapter-name spidapter \
  --system-adapter-stdio-cmd ~/code/spiceai/spiceai/target/release/spidapter \
  --system-adapter-stdio-args "stdio --verbose" \
  --system-adapter-env "SPIDAPTER_BACKEND=local" \
  --system-adapter-env "SPIDAPTER_NUM_EXECUTORS=4" \
  --system-adapter-env "SCHEDULER_STATE_LOCATION=$SCHEDULER_STATE_LOCATION" \
  --system-adapter-env "SPICED_LOG=info" \
  --concurrency 1 \
  --validate-results \
  --scrape-sut-metrics \
  2>&1 | tee "$RUN_LOG"
```

## 6. Verify success

A successful run should show:

- `Checkpoint 0 converged`
- `Checkpoint 1 converged`
- `Benchmark completed (outcome: success)`

Quick check:

```bash
rg -n "Checkpoint [01] converged|Benchmark completed" "$RUN_LOG"
```

## 7. Optional variations

Once the baseline run passes, these knobs are useful for stress-testing:

### Increase query concurrency

```bash
--concurrency 4
```

### Increase local executor count

```bash
SPIDAPTER_NUM_EXECUTORS=8
--system-adapter-env "SPIDAPTER_NUM_EXECUTORS=8"
```

### Increase ETL batch coalescing size

```bash
SPICEBENCH_TARGET_BATCH_ROWS=500000
```

Example:

```bash
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
SPIDAPTER_BACKEND=local \
SPIDAPTER_NUM_EXECUTORS=4 \
SPICEBENCH_TARGET_BATCH_ROWS=500000 \
SPICEBENCH_ADBC_UPDATE_STRATEGY=bulk_ingest_upsert \
SPICEBENCH_ADBC_DELETE_BATCH_SIZE=50000 \
SPICEBENCH_ADBC_REUSE_BULK_INGEST_STREAMS=false \
./target/release/spicebench run \
  ...
```

## 8. Troubleshooting

### `spidapter` cannot find `spiced`

Re-copy the locally-built binary:

```bash
mkdir -p ~/.spice/bin
cp ~/code/spiceai/spiceai/target/release/spiced ~/.spice/bin/spiced
```

### MinIO bucket is empty or missing

List the local bucket contents:

```bash
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://spicebench/
```

If `data-gen-mutable/tpch/0.1/` is missing, re-run the generate + checkpoint steps.

### Checkpoint validation does not start

Make sure these objects exist in MinIO:

- `data-gen-mutable/tpch/0.1/data.tar.zst`
- `data-gen-mutable/tpch/0.1/checkpoints.json`
- `data-gen-mutable/tpch/0.1/checkpoints/...`

### Mutable run fails with reusable ingest streams enabled

Keep this disabled for mutable local runs:

```bash
SPICEBENCH_ADBC_REUSE_BULK_INGEST_STREAMS=false
```

## 9. Inspect the running local cluster (optional)

While the benchmark is still running, you can query the scheduler directly.

Extract the dynamic scheduler port and API key from the run log:

```bash
SCHED_PORT=$(rg -o "launching scheduler process:.*--http 0.0.0.0:[0-9]+" "$RUN_LOG" | grep -o "[0-9]\+$")
RUN_ID=$(rg -o "setup: run_id=[a-f0-9-]+" "$RUN_LOG" | cut -d= -f2 | tail -1)
API_KEY="spidapter-local-$RUN_ID"
```

Run a query:

```bash
curl -s "http://127.0.0.1:$SCHED_PORT/v1/sql" \
  -H 'Content-Type: text/plain' \
  -H "X-API-Key: $API_KEY" \
  -d 'SELECT COUNT(*) FROM spicebench.bench.lineitem'
```

## 10. Clean up

Stop MinIO when you are done:

```bash
docker stop spicebench-minio
```

If you want to remove the container entirely:

```bash
docker rm -f spicebench-minio
```

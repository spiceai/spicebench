# ETL Pipeline

`etl` reads a generated archive, rehydrates records, and writes to either:

- S3 as hive-partitioned Parquet (default)
- an ADBC target via bulk ingest
- a null sink that discards writes for throughput benchmarking

Dataset configuration is read from the extracted `version.json` metadata written by `data-generation`.

## Required Inputs

Provide one of these source modes:

- `--archive-file <path>` to read a local `.tar.zst` archive
- `--bucket <bucket>` plus the S3 source flags to download the archive from S3

The version path is derived automatically from `--scale-factor`, so `--scale-factor 1` reads from the `1.0` version path.

## S3 Hive Sink (default)

Use `--sink s3-hive` to write hive-partitioned Parquet to S3.

- `--target-prefix`: Base S3 key prefix for ETL output. Defaults to the source prefix when empty.
- `--partition-by`: Comma-separated partition columns. Defaults to `__created_at`.

### Example

```bash
cargo run -p etl -- \
    --scenario tpch \
    --scale-factor 1 \
    --bucket peasee-indexes \
    --prefix raw \
    --target-prefix rehydrated \
    --partition-by __created_at
```

## ADBC Sink

Use `--sink adbc` to write via ADBC bulk ingest.

- `--adbc-driver`: ADBC driver name such as `databricks` or `flightsql`
- `--adbc-uri`: Connection URI passed as the ADBC database `uri` option
- `--adbc-catalog`: Optional target catalog
- `--adbc-schema`: Optional target schema
- `--adbc-option key=value`: Additional ADBC database options. Repeatable.
- `--adbc-create-tables`: Create tables from dataset schemas before ETL starts

When `--adbc-driver flightsql` is used, ETL defaults `adbc.flight.sql.client_option.with_max_msg_size` to `78643200` (75 MiB) unless you override it explicitly.

### Databricks Example

```bash
cargo run -p etl -- \
    --scenario tpch \
    --scale-factor 1 \
    --bucket peasee-indexes \
    --prefix raw \
    --adbc-driver databricks \
    --adbc-uri "databricks://token:${DATABRICKS_TOKEN}@${DATABRICKS_ENDPOINT}:443/${DATABRICKS_HTTP_PATH}" \
    --adbc-catalog main \
    --adbc-schema tpch \
    --adbc-create-tables
```

### FlightSQL Example

```bash
cargo run -p etl -- \
    --scenario tpch \
    --scale-factor 1 \
    --bucket peasee-indexes \
    --prefix raw \
    --adbc-driver flightsql \
    --adbc-uri "grpcs://${SPICE_CLOUD_FLIGHTSQL_HOST}:443" \
    --adbc-create-tables \
    --adbc-option username="" \
    --adbc-option password="${SPICE_CLOUD_API_KEY}"
```

## Null Sink

Use `--sink null` to discard all ETL writes. This is useful for measuring source and ETL throughput without sink overhead.

```bash
cargo run -p etl -- \
    --scenario tpch \
    --scale-factor 1 \
    --archive-file ./tpch-sf1.tar.zst \
    --sink null
```

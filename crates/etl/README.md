# ETL Pipeline

`etl` reads raw batches from S3, rehydrates records (for example adding a time column), and writes to either:

- S3 as hive-partitioned Parquet (default), or
- an ADBC target via bulk ingest, or
- a null sink that discards writes for throughput benchmarking.

Dataset configuration (dataset type, scale factor, number of steps, mutations) is read automatically from the `version.json` metadata written by the data generation tool.

## Required arguments

- `--bucket`: S3 bucket containing source batches.
- `--prefix`: S3 prefix (the `{prefix}` portion of `{prefix}/{scenario}/{version}/`).
- `--scenario`: Scenario name (default: `tpch`).
- `--version`: Version identifier for the data generation to read from.

## S3 Hive sink (default)

Use `--sink s3-hive` (default) to write hive-partitioned Parquet to S3.

- `--target-prefix`: Base S3 key prefix for ETL output (defaults to `--prefix`).
- `--partition-by`: Comma-separated partition columns (default: `__created_at`).

### S3 Hive example

```bash
cargo run -p etl -- \
	--scenario tpch \
	--version 1 \
	--bucket peasee-indexes \
	--prefix raw \
	--target-prefix rehydrated \
	--partition-by __created_at
```

## ADBC sink (optional)

Use `--sink adbc` to write via ADBC bulk ingest.

- `--adbc-driver`: ADBC driver name (for example `databricks` or `flightsql`).
- `--adbc-uri`: Connection URI passed as ADBC database option `uri`.
- `--adbc-option key=value`: Additional ADBC database option (repeatable).
- `--adbc-create-tables`: Send PostgreSQL-compatible `CREATE TABLE IF NOT EXISTS` statements before ETL starts, using dataset table schemas (including `__created_at`).

When `--adbc-driver flightsql` is used, ETL defaults
`adbc.flight.sql.client_option.with_max_msg_size` to `78643200` (75 MiB)
unless you explicitly provide that option via `--adbc-option`.

When using ADBC output, provide both `--adbc-driver` and `--adbc-uri`.

## Null sink (throughput benchmark)

Use `--sink null` to discard all ETL writes (`/dev/null` style). This is useful for measuring source + ETL pipeline throughput without sink/storage overhead.

### Null sink example

```bash
cargo run -p etl -- \
	--scenario tpch \
	--version 1 \
	--bucket peasee-indexes \
	--prefix raw \
	--sink null
```

### Databricks example

```bash
cargo run -p etl -- \
	--scenario tpch \
	--version 1 \
	--bucket peasee-indexes \
	--prefix raw \
	--region us-west-2 \
	--adbc-driver databricks \
	--adbc-uri "databricks://token:${DATABRICKS_TOKEN}@${DATABRICKS_ENDPOINT}:443/${DATABRICKS_HTTP_PATH}" \
	--adbc-create-tables \
	--adbc-option some_driver_specific_option=some_value \
	--adbc-schema tpch
```

### Spice Cloud example (FlightSQL)

```bash
cargo run -p etl -- \
	--scenario tpch \
	--version 1 \
	--bucket peasee-indexes \
	--prefix raw \
	--adbc-driver flightsql \
	--adbc-uri "grpcs://${SPICE_CLOUD_FLIGHTSQL_HOST}:443" \
	--adbc-create-tables \
	--adbc-option username="" \
	--adbc-option password="${SPICE_CLOUD_API_KEY}"
```

Use the FlightSQL endpoint and credentials from your Spice Cloud deployment/run configuration.
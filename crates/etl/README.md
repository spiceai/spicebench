# ETL Pipeline

`etl` reads raw batches from S3, rehydrates records (for example adding a time column), and writes directly to the destination system through a single ADBC sink.

Dataset configuration (dataset type, scale factor, number of steps, mutations) is read automatically from the `version.json` metadata written by the data generation tool.

## Required arguments

- `--bucket`: S3 bucket containing source batches.
- `--prefix`: S3 prefix (the `{prefix}` portion of `{prefix}/{scenario}/{version}/`).
- `--scenario`: Scenario name (default: `tpch`).
- `--version`: Version identifier for the data generation to read from.
- `--adbc-driver`: ADBC driver name (for example `databricks` or `flightsql`).
- `--adbc-uri`: Connection URI passed as ADBC database option `uri`.

## Databricks example

```bash
cargo run -p etl -- \
	--scenario tpch \
	--version 1 \
	--bucket peasee-indexes \
	--prefix raw \
	--region us-west-2 \
	--adbc-driver databricks \
	--adbc-uri "databricks://token:${DATABRICKS_TOKEN}@${DATABRICKS_ENDPOINT}:443/${DATABRICKS_HTTP_PATH}" \
	--adbc-schema tpch
```

## Spice Cloud example (FlightSQL)

```bash
cargo run -p etl -- \
	--scenario tpch \
	--version 1 \
	--bucket peasee-indexes \
	--prefix raw \
	--adbc-driver flightsql \
	--adbc-uri "grpcs://${SPICE_CLOUD_FLIGHTSQL_HOST}:443"
```

Use the FlightSQL endpoint and credentials from your Spice Cloud deployment/run configuration.
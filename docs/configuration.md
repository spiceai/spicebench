# Configuration

SpiceBench uses **Spicepod YAML** files for dataset and infrastructure configuration, and CLI flags for runtime behavior.

## Spicepod Format

Spicepod is a declarative YAML configuration format. SpiceBench uses it to define datasets, catalogs, and runtime settings.

### Basic Structure

```yaml
version: v1
kind: Spicepod
name: my-benchmark

datasets:
  - name: customer
    from: s3://my-bucket/tpch/customer/customer.parquet
    params:
      file_format: parquet
      s3_auth: public

  - name: orders
    from: s3://my-bucket/tpch/orders/orders.parquet
    params:
      file_format: parquet
      s3_auth: public

runtime:
  # Runtime configuration options
```

### Spicepod Fields

| Field          | Type              | Required | Description                        |
| -------------- | ----------------- | -------- | ---------------------------------- |
| `version`      | `v1beta1` \| `v1` | Yes      | Spicepod format version            |
| `kind`         | `Spicepod`        | Yes      | Must be `Spicepod`                 |
| `name`         | String            | Yes      | Name of the pod                    |
| `datasets`     | List              | No       | Dataset definitions                |
| `catalogs`     | List              | No       | Catalog definitions                |
| `views`        | List              | No       | View definitions                   |
| `models`       | List              | No       | Model definitions                  |
| `embeddings`   | List              | No       | Embedding definitions              |
| `runtime`      | Object            | No       | Runtime configuration              |
| `management`   | Object            | No       | Management settings                |
| `secrets`      | List              | No       | Secret references                  |
| `extensions`   | List              | No       | Extension configurations           |
| `dependencies` | List              | No       | References to other Spicepod files |

### Dataset Definition

```yaml
datasets:
  - name: customer                    # Table name
    from: s3://bucket/path/file.parquet  # Data source URI
    params:                           # Source-specific parameters
      file_format: parquet
      s3_auth: public
    acceleration:                     # Optional acceleration config
      enabled: true
      engine: duckdb
```

#### Dataset Fields

| Field          | Type                  | Description                                      |
| -------------- | --------------------- | ------------------------------------------------ |
| `name`         | String                | Table name used in queries                       |
| `from`         | String                | Data source URI (e.g., `s3://`, `databricks://`) |
| `params`       | Map\<String, String\> | Source-specific parameters                       |
| `acceleration` | Object                | Acceleration/caching configuration               |
| `time_column`  | String                | Column for temporal ordering                     |
| `primary_key`  | String                | Primary key column(s)                            |

### TPC-H Example (public S3)

```yaml
version: v1
kind: Spicepod
name: s3-public[parquet]-federated

datasets:
  - from: s3://spiceai-public-datasets/tpch/customer/customer.parquet
    name: customer
    params: &s3_params
      file_format: parquet
      s3_auth: public
  - from: s3://spiceai-public-datasets/tpch/lineitem/lineitem.parquet
    name: lineitem
    params: *s3_params
  - from: s3://spiceai-public-datasets/tpch/nation/nation.parquet
    name: nation
    params: *s3_params
  - from: s3://spiceai-public-datasets/tpch/orders/orders.parquet
    name: orders
    params: *s3_params
  - from: s3://spiceai-public-datasets/tpch/part/part.parquet
    name: part
    params: *s3_params
  - from: s3://spiceai-public-datasets/tpch/partsupp/partsupp.parquet
    name: partsupp
    params: *s3_params
  - from: s3://spiceai-public-datasets/tpch/region/region.parquet
    name: region
    params: *s3_params
  - from: s3://spiceai-public-datasets/tpch/supplier/supplier.parquet
    name: supplier
    params: *s3_params
```

## Query Sets

SpiceBench ships with several built-in query sets:

| Query Set           | Flag                              | Description                                  |
| ------------------- | --------------------------------- | -------------------------------------------- |
| TPC-H               | `--query-set tpch`                | 22 standard TPC-H analytical queries         |
| TPC-DS              | `--query-set tpcds`               | Standard TPC-DS decision support queries     |
| ClickBench          | `--query-set clickbench`          | ClickBench web analytics queries             |
| Parameterized TPC-H | `--query-set tpch[parameterized]` | TPC-H with randomized parameter substitution |
| Scenario            | `--query-set scenario`            | Custom queries from file                     |

### Custom Query Files

Use `--query-set scenario --scenario-query-file path/to/queries.sql` to load custom queries. The file should contain SQL statements separated by semicolons.

## SQL Dialect Overrides

SpiceBench rewrites SQL queries for different database engines using `--query-overrides`. This handles syntax differences like quoting, function names, type casting, and reserved words.

| Dialect               | Use When Targeting                 |
| --------------------- | ---------------------------------- |
| `sqlite`              | SQLite                             |
| `postgresql`          | PostgreSQL                         |
| `mysql`               | MySQL                              |
| `dremio`              | Dremio                             |
| `spark`               | Apache Spark SQL                   |
| `duckdb`              | DuckDB                             |
| `duckdb-zero-results` | DuckDB (empty result variant)      |
| `duckdb-partitioned`  | DuckDB (partitioned tables)        |
| `snowflake`           | Snowflake                          |
| `oracle`              | Oracle                             |
| `odbc-athena`         | Amazon Athena via ODBC             |
| `odbc-databricks`     | Databricks via ODBC                |
| `iceberg-sf1`         | Iceberg tables (SF1)               |
| `iceberg-hadoop`      | Iceberg with Hadoop catalog        |
| `spicecloud-catalog`  | Spice Cloud with catalog namespace |
| `glue-catalog`        | AWS Glue Data Catalog              |
| `databricks-catalog`  | Databricks Unity Catalog           |
| `spicecloud`          | Spice Cloud                        |
| `dynamodb`            | Amazon DynamoDB                    |

Example:

```bash
spicebench --query-set tpch --query-overrides spark ...
```

## Table Format

The `--table-format` flag declares the storage format for benchmark tables:

| Format  | Value     | Description              |
| ------- | --------- | ------------------------ |
| Parquet | `parquet` | Apache Parquet (default) |
| Iceberg | `iceberg` | Apache Iceberg           |
| Delta   | `delta`   | Delta Lake               |

This value is passed to the system adapter in `setup` metadata and used during `create_tables` to create tables in the appropriate format.

## Run Metadata

SpiceBench attaches metadata to each run for cross-system comparison:

| Field                    | CLI Flag                   | Default                   | Description              |
| ------------------------ | -------------------------- | ------------------------- | ------------------------ |
| `table_format`           | `--table-format`           | `parquet`                 | Dataset table format     |
| `executor_instance_type` | `--executor-instance-type` | `unknown`                 | Executor hardware class  |
| `scenario`               | `--scenario`               | `tpch`                    | Benchmark scenario       |
| `system_under_test`      | `--system-adapter-name`    | —                         | Target system identifier |
| `etl_bucket`             | `--etl-bucket`             | `spiceai-public-datasets` | Source data bucket       |
| `etl_prefix`             | `--etl-prefix`             | `data-gen`                | Source data prefix       |
| `etl_version`            | `--etl-version`            | `1`                       | Data generation version  |

All metadata is sent to the adapter in the `setup` request and attached as OTel resource attributes on exported metrics.

## Spicepod Loading

The `spicepod` crate supports loading configuration from multiple sources:

| Source     | Method                                  | Example                        |
| ---------- | --------------------------------------- | ------------------------------ |
| Local file | `Spicepod::load(path)`                  | `./spicepod.yaml`              |
| S3         | `Spicepod::load_from_object_store(url)` | `s3://bucket/spicepod.yaml`    |
| GCS        | `Spicepod::load_from_object_store(url)` | `gs://bucket/spicepod.yaml`    |
| Azure      | `Spicepod::load_from_object_store(url)` | `az://container/spicepod.yaml` |

### Dependencies

Spicepod files can reference other Spicepod files via `dependencies`:

```yaml
version: v1
kind: Spicepod
name: main
dependencies:
  - path: ./common-datasets.yaml
```

The `App` struct aggregates all components from the root pod and its transitive dependencies into a single configuration object.

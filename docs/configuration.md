# Configuration

SpiceBench uses CLI flags for runtime behavior. See the [CLI Reference](cli-reference.md) for a complete list of flags and options.

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

## SQL Overrides

SpiceBench supports SQL query rewrites for supported systems using `--query-overrides`.

| Dialect              | Use When Targeting                 |
| -------------------- | ---------------------------------- |
| `odbc-databricks`    | Databricks SQL via ODBC            |
| `databricks-catalog` | Databricks Unity Catalog           |
| `spicecloud`         | Spice Cloud                        |
| `spicecloud-catalog` | Spice Cloud with catalog namespace |

Example:

```bash
spicebench --query-set tpch --query-overrides databricks-catalog ...
```

## Table Format

The `--table-format` flag declares the storage format for benchmark tables:

| Format  | Value     | Description              |
| ------- | --------- | ------------------------ |
| Parquet | `parquet` | Apache Parquet (default) |
| Iceberg | `iceberg` | Apache Iceberg           |
| Delta   | `delta`   | Delta Lake               |

This value is passed to the system adapter in `setup` metadata for table creation in the appropriate format.

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

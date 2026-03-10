# Configuration

SpiceBench uses CLI flags for runtime behavior. See the [CLI Reference](cli-reference.md) for the complete flag list.

## Benchmark Scenario

The current main `spicebench` binary exposes one built-in benchmark scenario:

| Scenario | Flag              | Description                                |
| -------- | ----------------- | ------------------------------------------ |
| TPC-H    | `--scenario tpch` | Built-in TPC-H scenario and query workload |

The main binary does not currently expose separate `--query-set`, `--scenario-query-file`, or `--query-overrides` flags.

## Table Format

The `--table-format` flag declares the storage format for benchmark tables:

| Format  | Value     | Description              |
| ------- | --------- | ------------------------ |
| Parquet | `parquet` | Apache Parquet (default) |
| Iceberg | `iceberg` | Apache Iceberg           |
| Delta   | `delta`   | Delta Lake               |

This value is passed to the system adapter in `setup` metadata for table creation in the appropriate format.

## Run Metadata

SpiceBench attaches metadata to each run for cross-system comparison and adapter setup:

| Field                      | CLI Flag / Source             | Default / Behavior                  | Description                              |
| -------------------------- | ----------------------------- | ----------------------------------- | ---------------------------------------- |
| `table_format`             | `--table-format`              | `parquet`                           | Dataset table format                     |
| `executor_instance_type`   | `--executor-instance-type`    | `unknown`                           | Executor hardware class                  |
| `scenario`                 | `--scenario`                  | required                            | Benchmark scenario                       |
| `system_adapter_name`      | `--system-adapter-name`       | `system_adapter`                    | Logical target-system identifier         |
| `etl_bucket`               | `--etl-bucket`                | `spiceai-public-datasets`           | Source data bucket                       |
| `etl_prefix`               | `--etl-prefix`                | `data-gen`                          | Source data prefix                       |
| `etl_version`              | derived from `--scale-factor` | `format_scale_factor(scale_factor)` | Version path segment passed to adapters  |
| `scale_factor`             | `--scale-factor`              | `1.0`                               | Benchmark dataset scale factor           |
| `scheduler_state_location` | `--scheduler-state-location`  | omitted unless provided             | Optional shared scheduler-state location |

SpiceBench sends this metadata to the adapter in the `setup` request. A subset is also attached to exported metrics as resource attributes or per-metric attributes, depending on the metric pipeline.

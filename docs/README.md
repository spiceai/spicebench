# SpiceBench Documentation

Detailed documentation for SpiceBench - an end-to-end benchmark for data & AI platforms focused on operational data.

## Table of Contents

| Document                                            | Description                                                                                        |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| [Architecture](architecture.md)                     | High-level system architecture, run lifecycle, benchmark phases, and data flow                     |
| [Getting Started](getting-started.md)               | Installation, first run, prerequisites, and quick-start examples                                   |
| [CLI Reference](cli-reference.md)                   | Complete `spicebench run`, `generate`, `etl`, and `checkpoint` CLI flags and options               |
| [System Adapters](system-adapters.md)               | JSON-RPC 2.0 adapter protocol, transport modes, and current supported systems                      |
| [Data Generation & ETL](data-generation-and-etl.md) | Dataset generation, ETL pipeline, sinks, checkpointing, and S3 layout                              |
| [Metrics & Telemetry](metrics-and-telemetry.md)     | All OTel instruments, streaming metrics, SUT scraping, Arrow Flight export, and Grafana dashboards |
| [Configuration](configuration.md)                   | Scenario, table format, and run metadata                                                           |
| [Crate Reference](crate-reference.md)               | Per-crate API overview for all workspace crates                                                    |

## Quick Links

- [Main README](../README.md)
- [Metrics Reference (METRICS.md)](../METRICS.md)
- [Grafana Dashboard](../dashboards/spicebench-benchmarks.grafana.json)
- [SpiceBench.com](https://spicebench.com)

## Future Ideas

SpiceBench currently benchmarks ingestion-to-query operational performance. Planned extensions include:

- **Warm-up phase** - run the benchmark workload once before timing starts to eliminate cold-start variance
- text-to-SQL quality and latency
- search/retrieval quality and freshness under write pressure
- context engineering quality, token efficiency, and end-to-end readiness latency

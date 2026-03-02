# SpiceBench Documentation

Detailed documentation for SpiceBench — an end-to-end benchmark for data & AI platforms focused on operational data.

## Table of Contents

| Document                                            | Description                                                                                        |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| [Architecture](architecture.md)                     | High-level system architecture, run lifecycle, benchmark phases, and data flow                     |
| [Getting Started](getting-started.md)               | Installation, first run, prerequisites, and quick-start examples                                   |
| [CLI Reference](cli-reference.md)                   | Complete `spicebench` and `data-generation` CLI flags and options                                  |
| [System Adapters](system-adapters.md)               | JSON-RPC 2.0 adapter protocol, transport modes, and how to build a new adapter                     |
| [Data Generation & ETL](data-generation-and-etl.md) | Dataset generation, ETL pipeline, sinks, checkpointing, and S3 layout                              |
| [Metrics & Telemetry](metrics-and-telemetry.md)     | All OTel instruments, streaming metrics, SUT scraping, Arrow Flight export, and Grafana dashboards |
| [Configuration](configuration.md)                   | Spicepod YAML format, dataset definitions, query sets, and SQL dialect overrides                   |
| [Crate Reference](crate-reference.md)               | Per-crate API overview for all workspace crates                                                    |

## Quick Links

- [Main README](../README.md)
- [Metrics Tracking (METRICS.md)](../METRICS.md)
- [System Adapter Templates](../system-adapters/templates/README.md)
- [Grafana Dashboard](../dashboards/spicebench-benchmarks.grafana.json)
- [SpiceBench.com](https://spicebench.com)

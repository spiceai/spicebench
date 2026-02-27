# Session Context

## User Prompts

### Prompt 1

Check all the metrics in here src/metrics.rs. I don't know if theyre using the write gauge/counter etc

### Prompt 2

should `SUT_MEMORY_USAGE_BYTES` be a gauge?

### Prompt 3

Should `SUT_DISK_*` be a gauge?

### Prompt 4

theyre cumulative, update to counters for me

### Prompt 5

Okaym just ran this and im only getting a single datapoint in grafana when scrapped. I should be getting multiple, per 5 seconds, in spawn_sut_metrics_scraper.

### Prompt 6

yeah that's wrong. updarte the MetricsResponse to treat them as counters, then in `spawn_sut_metrics_scraper` handle it too


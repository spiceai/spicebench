export interface BenchmarkRun {
  id: string;
  system: string;
  systemSlug: string;
  timestamp: string;
  gitSha: string;
  version: string;

  summary: {
    e2eDurationSeconds: number;
    ingestionRecordsPerSecond: number;
    dataSizeBytes: number;
    efficiencyCores: number;
    totalQueries: number;
    queriesPerSecond: number;
  };

  queries: Array<{
    queryId: string;
    queryName: string;
    latencyP50Ms: number;
    latencyP95Ms: number;
    latencyP99Ms: number;
    executionCount: number;
  }>;

  resourceUsage: {
    avgCpuPercent: number;
    avgMemoryPercent: number;
    avgDiskIops: number;
    peakMemoryBytes: number;
  };

  e2eEventLatency: {
    p50Ms: number;
    p95Ms: number;
    p99Ms: number;
  };
}

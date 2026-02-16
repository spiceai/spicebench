import Link from "next/link";
import { notFound } from "next/navigation";
import { getAllRuns, getRunById } from "@/lib/results";

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}m ${s.toFixed(1)}s`;
}

function formatBytes(bytes: number): string {
  if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(1)} GB`;
  if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(1)} MB`;
  return `${(bytes / 1e3).toFixed(1)} KB`;
}

function formatNumber(n: number): string {
  return n.toLocaleString("en-US");
}

export function generateStaticParams() {
  const runs = getAllRuns();
  return runs.map((run) => ({ id: run.id }));
}

export default async function RunDetail({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const run = getRunById(id);
  if (!run) notFound();

  const summaryCards = [
    { label: "E2E Benchmark Duration", value: formatDuration(run.summary.e2eDurationSeconds) },
    { label: "Ingestion Rate", value: `${formatNumber(run.summary.ingestionRecordsPerSecond)} records/s` },
    { label: "Total Data Ingested", value: formatBytes(run.summary.dataSizeBytes) },
    { label: "Compute Cores", value: `${run.summary.efficiencyCores} cores` },
    { label: "Total Queries Executed", value: formatNumber(run.summary.totalQueries) },
    { label: "Query Throughput", value: `${run.summary.queriesPerSecond.toFixed(1)} queries/s` },
  ];

  return (
    <div>
      <Link
        href="/"
        className="text-sm text-accent hover:text-accent-hover transition-colors"
      >
        &larr; Back to leaderboard
      </Link>

      {/* Header */}
      <div className="mt-4 mb-8">
        <h1 className="text-2xl font-bold flex items-center gap-3">
          <img
            src={`/logos/${run.systemSlug}.svg`}
            alt={run.system}
            width={28}
            height={28}
          />
          {run.system}
        </h1>
        <div className="flex flex-wrap gap-x-4 gap-y-1 mt-1 text-sm text-text-secondary">
          <span>Version {run.version}</span>
          <span>{new Date(run.timestamp).toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" })}</span>
          <span>
            Commit: <code className="text-xs">{run.gitSha}</code>
          </span>
        </div>
      </div>

      {/* Summary Metrics */}
      <section className="mb-8">
        <h2 className="text-lg font-semibold mb-3">Summary Metrics</h2>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          {summaryCards.map((card) => (
            <div
              key={card.label}
              className="bg-bg-card border border-border rounded-lg p-4"
            >
              <div className="text-xs text-text-secondary mb-1">
                {card.label}
              </div>
              <div className="text-lg font-semibold tabular-nums">
                {card.value}
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* Per-Query P99 Latency */}
      <section className="mb-8">
        <h2 className="text-lg font-semibold mb-3">Query P99 Latency</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-left text-text-secondary">
                <th className="py-3 pr-4 font-medium">Query ID</th>
                <th className="py-3 pr-4 font-medium">Query Name</th>
                <th className="py-3 pr-4 font-medium text-right">P99 Latency (ms)</th>
                <th className="py-3 font-medium text-right">Execution Count</th>
              </tr>
            </thead>
            <tbody>
              {run.queries.map((q) => (
                <tr
                  key={q.queryId}
                  className="border-b border-border"
                >
                  <td className="py-3 pr-4 font-mono text-xs">
                    {q.queryId}
                  </td>
                  <td className="py-3 pr-4">{q.queryName}</td>
                  <td className="py-3 pr-4 text-right tabular-nums">
                    {q.latencyP99Ms.toFixed(1)}
                  </td>
                  <td className="py-3 text-right tabular-nums">
                    {formatNumber(q.executionCount)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* E2E Event P99 Latency */}
      <section className="mb-8">
        <h2 className="text-lg font-semibold mb-3">
          E2E Event P99 Latency
        </h2>
        <p className="text-xs text-text-secondary mb-3">
          Time from event creation to the event being queryable (P99)
        </p>
        <div className="bg-bg-card border border-border rounded-lg p-4 inline-block">
          <div className="text-xs text-text-secondary mb-1">
            Event P99 Latency
          </div>
          <div className="text-lg font-semibold tabular-nums">
            {run.e2eEventLatency.p99Ms.toFixed(1)} ms
          </div>
        </div>
      </section>

      {/* Resource Usage */}
      <section className="mb-8">
        <h2 className="text-lg font-semibold mb-3">Resource Usage</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div className="bg-bg-card border border-border rounded-lg p-4">
            <div className="text-xs text-text-secondary mb-1">Average CPU Utilization</div>
            <div className="text-lg font-semibold tabular-nums">
              {run.resourceUsage.avgCpuPercent.toFixed(1)}%
            </div>
          </div>
          <div className="bg-bg-card border border-border rounded-lg p-4">
            <div className="text-xs text-text-secondary mb-1">Average Memory Utilization</div>
            <div className="text-lg font-semibold tabular-nums">
              {run.resourceUsage.avgMemoryPercent.toFixed(1)}%
            </div>
          </div>
          <div className="bg-bg-card border border-border rounded-lg p-4">
            <div className="text-xs text-text-secondary mb-1">Average Disk IOPS</div>
            <div className="text-lg font-semibold tabular-nums">
              {formatNumber(run.resourceUsage.avgDiskIops)}
            </div>
          </div>
          <div className="bg-bg-card border border-border rounded-lg p-4">
            <div className="text-xs text-text-secondary mb-1">Peak Memory Usage</div>
            <div className="text-lg font-semibold tabular-nums">
              {formatBytes(run.resourceUsage.peakMemoryBytes)}
            </div>
          </div>
        </div>
      </section>

      <Link
        href="/"
        className="text-sm text-accent hover:text-accent-hover transition-colors"
      >
        &larr; Back to leaderboard
      </Link>
    </div>
  );
}

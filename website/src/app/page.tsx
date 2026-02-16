import Link from "next/link";
import { getAllRuns } from "@/lib/results";
import type { BenchmarkRun } from "@/lib/types";

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

function avgQueryP99(run: BenchmarkRun): number {
  if (run.queries.length === 0) return 0;
  const sum = run.queries.reduce((acc, q) => acc + q.latencyP99Ms, 0);
  return sum / run.queries.length;
}

function rankClass(rank: number): string {
  if (rank === 1) return "text-rank-gold font-bold";
  if (rank === 2) return "text-rank-silver font-bold";
  if (rank === 3) return "text-rank-bronze font-bold";
  return "text-text-secondary";
}

function systemSpec(run: BenchmarkRun): string {
  return `${run.summary.efficiencyCores} cores, ${formatBytes(run.summary.dataSizeBytes)}`;
}

function SystemLogo({ slug, name }: { slug: string; name: string }) {
  return (
    <img
      src={`/logos/${slug}.svg`}
      alt={name}
      width={20}
      height={20}
      className="inline-block shrink-0"
    />
  );
}

export default function Leaderboard() {
  const runs = getAllRuns();
  const maxDuration =
    runs.length > 0
      ? Math.max(...runs.map((r) => r.summary.e2eDurationSeconds))
      : 1;

  return (
    <div>
      <h1 className="text-2xl font-bold mb-1">Leaderboard</h1>
      <p className="text-text-secondary mb-6">
        Ranked by E2E benchmark duration (lower is better)
      </p>

      {runs.length === 0 ? (
        <p className="text-text-secondary">No benchmark results yet.</p>
      ) : (
        <>
          {/* Bar Chart */}
          <div className="mb-8">
            <h2 className="text-sm font-medium text-text-secondary mb-3">
              E2E Benchmark Duration
            </h2>
            <div className="flex flex-col gap-2">
              {runs.map((run) => {
                const pct =
                  (run.summary.e2eDurationSeconds / maxDuration) * 100;
                return (
                  <Link
                    key={run.id}
                    href={`/runs/${run.id}`}
                    className="group flex items-center gap-3"
                  >
                    <div className="w-56 shrink-0 flex items-center gap-2 text-sm">
                      <SystemLogo slug={run.systemSlug} name={run.system} />
                      <span className="truncate group-hover:text-accent transition-colors">
                        {run.system}{" "}
                        <span className="text-text-secondary font-normal">
                          ({systemSpec(run)})
                        </span>
                      </span>
                    </div>
                    <div className="flex-1 h-7 rounded bg-bar-bg relative">
                      <div
                        className="h-full rounded bg-bar transition-all"
                        style={{ width: `${pct}%` }}
                      />
                      <span className="absolute right-2 top-1/2 -translate-y-1/2 text-xs tabular-nums font-medium text-text-primary">
                        {formatDuration(run.summary.e2eDurationSeconds)}
                      </span>
                    </div>
                  </Link>
                );
              })}
            </div>
          </div>

          {/* Table */}
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-text-secondary">
                  <th className="py-3 pr-4 font-medium">Rank</th>
                  <th className="py-3 pr-4 font-medium">System</th>
                  <th className="py-3 pr-4 font-medium text-right">
                    E2E Benchmark Duration
                  </th>
                  <th className="py-3 pr-4 font-medium text-right">
                    Ingestion Records/s
                  </th>
                  <th className="py-3 pr-4 font-medium text-right">
                    Query P99 Latency
                  </th>
                  <th className="py-3 pr-4 font-medium text-right">
                    E2E Event P99 Latency
                  </th>
                </tr>
              </thead>
              <tbody>
                {runs.map((run, i) => {
                  const rank = i + 1;
                  return (
                    <tr
                      key={run.id}
                      className="border-b border-border hover:bg-bg-hover transition-colors"
                    >
                      <td
                        className={`py-3 pr-4 tabular-nums ${rankClass(rank)}`}
                      >
                        {rank}
                      </td>
                      <td className="py-3 pr-4">
                        <Link
                          href={`/runs/${run.id}`}
                          className="inline-flex items-center gap-2 text-accent hover:text-accent-hover font-medium transition-colors"
                        >
                          <SystemLogo slug={run.systemSlug} name={run.system} />
                          {run.system}
                        </Link>
                        <span className="text-text-secondary text-xs ml-2">
                          ({systemSpec(run)})
                        </span>
                        <span className="text-text-secondary text-xs ml-1">
                          v{run.version}
                        </span>
                      </td>
                      <td className="py-3 pr-4 text-right tabular-nums font-medium">
                        {formatDuration(run.summary.e2eDurationSeconds)}
                      </td>
                      <td className="py-3 pr-4 text-right tabular-nums">
                        {formatNumber(run.summary.ingestionRecordsPerSecond)}
                      </td>
                      <td className="py-3 pr-4 text-right tabular-nums">
                        {avgQueryP99(run).toFixed(1)} ms
                      </td>
                      <td className="py-3 pr-4 text-right tabular-nums">
                        {run.e2eEventLatency.p99Ms.toFixed(1)} ms
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

import fs from "node:fs";
import path from "node:path";
import type { BenchmarkRun } from "./types";

const RESULTS_DIR = path.join(process.cwd(), "public", "results");

export function getAllRuns(): BenchmarkRun[] {
  if (!fs.existsSync(RESULTS_DIR)) return [];
  const files = fs.readdirSync(RESULTS_DIR).filter((f) => f.endsWith(".json"));
  const runs = files.map((file) => {
    const raw = fs.readFileSync(path.join(RESULTS_DIR, file), "utf-8");
    return JSON.parse(raw) as BenchmarkRun;
  });
  return runs.sort(
    (a, b) => a.summary.e2eDurationSeconds - b.summary.e2eDurationSeconds,
  );
}

export function getRunById(id: string): BenchmarkRun | undefined {
  const runs = getAllRuns();
  return runs.find((r) => r.id === id);
}

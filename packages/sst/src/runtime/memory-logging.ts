import os from "os";
import { createDebugFileLogger, DebugFileLogger } from "./debug-file-logger.js";
import { DEBUG_MEMORY, POOL_SIZE, WORKER_CONCURRENCY } from "./worker-config.js";

/**
 * Memory tracking for `sst dev`, enabled with SST_DEBUG_MEMORY=true.
 *
 * Every SAMPLE_INTERVAL_MS a line goes to .sst/memory.log with the parent
 * process's memory, the number of live workers and in-flight invocations, and
 * the sum of what the workers last reported for themselves. Node workers report
 * their own heap through `parentPort` after they load the bundle and after
 * each response (see support/nodejs-runtime). Peaks are folded into the pool
 * session summary so a run can be compared with the previous version at a glance.
 */

const MEMORY_LOG_FILE = process.env.SST_DEBUG_MEMORY_FILE || ".sst/memory.log";
const SAMPLE_INTERVAL_MS = 10_000;

export interface WorkerMemoryReport {
  rss: number;
  heapUsed: number;
  heapTotal: number;
  external: number;
  /** "loaded" right after the bundle import, "response" after an invocation. */
  phase: "loaded" | "response";
  loadMs?: number;
}

interface WorkerStatsProvider {
  (): { workers: number; inFlight: number; queued: number };
}

const mb = (bytes: number) => Math.round(bytes / 1024 / 1024);

let logger: DebugFileLogger | null = null;
let timer: NodeJS.Timeout | undefined;
const workerReports = new Map<string, WorkerMemoryReport>();
const peaks = {
  rss: 0,
  heapUsed: 0,
  workers: 0,
  inFlight: 0,
  queued: 0,
  workerHeap: 0,
  firstLoadMs: undefined as number | undefined,
};

function getLogger(): DebugFileLogger | null {
  if (!DEBUG_MEMORY) return null;
  if (!logger) {
    logger = createDebugFileLogger({
      filePath: MEMORY_LOG_FILE,
      sessionName: "MEMORY",
      sessionHeader: `POOL_SIZE=${POOL_SIZE} CONCURRENCY=${WORKER_CONCURRENCY} totalMem=${mb(os.totalmem())}MB`,
      width: 100,
    });
  }
  return logger;
}

export const isMemoryTrackingEnabled = () => DEBUG_MEMORY;

/** Called by the Node handler whenever a worker posts a memory report. */
export function recordWorkerMemory(workerID: string, report: WorkerMemoryReport) {
  if (!DEBUG_MEMORY) return;
  workerReports.set(workerID, report);
  if (report.phase === "loaded" && peaks.firstLoadMs === undefined) {
    peaks.firstLoadMs = report.loadMs;
  }
  const log = getLogger();
  log?.log("WORKER", {
    worker: workerID.slice(0, 8),
    phase: report.phase,
    heapUsedMB: mb(report.heapUsed),
    heapTotalMB: mb(report.heapTotal),
    externalMB: mb(report.external),
    ...(report.loadMs !== undefined ? { loadMs: report.loadMs } : {}),
  });
}

/** Called by the Node handler when a worker goes away. */
export function forgetWorkerMemory(workerID: string) {
  workerReports.delete(workerID);
}

function sample(stats: WorkerStatsProvider) {
  const log = getLogger();
  if (!log) return;
  const usage = process.memoryUsage();
  const { workers, inFlight, queued } = stats();
  let workerHeap = 0;
  for (const report of workerReports.values()) workerHeap += report.heapUsed;

  peaks.rss = Math.max(peaks.rss, usage.rss);
  peaks.heapUsed = Math.max(peaks.heapUsed, usage.heapUsed);
  peaks.workers = Math.max(peaks.workers, workers);
  peaks.inFlight = Math.max(peaks.inFlight, inFlight);
  peaks.queued = Math.max(peaks.queued, queued);
  peaks.workerHeap = Math.max(peaks.workerHeap, workerHeap);

  log.log("SAMPLE", {
    rssMB: mb(usage.rss),
    heapUsedMB: mb(usage.heapUsed),
    externalMB: mb(usage.external),
    workers,
    inFlight,
    queued,
    workerHeapMB: mb(workerHeap),
    freeMB: mb(os.freemem()),
  });
}

/** Start periodic sampling. No-op unless SST_DEBUG_MEMORY=true. */
export function startMemorySampling(stats: WorkerStatsProvider) {
  if (!DEBUG_MEMORY || timer) return;
  sample(stats);
  timer = setInterval(() => sample(stats), SAMPLE_INTERVAL_MS);
  timer.unref();
  process.on("exit", () => {
    clearInterval(timer);
    getLogger()?.close(memorySummary());
  });
}

/** Peak line for the pool session summary. Empty when tracking is off. */
export function memorySummary(): string {
  if (!DEBUG_MEMORY) return "";
  return (
    `  memory: peakRSS=${mb(peaks.rss)}MB peakHeap=${mb(peaks.heapUsed)}MB ` +
    `peakWorkers=${peaks.workers} peakInFlight=${peaks.inFlight} peakQueued=${peaks.queued} ` +
    `peakWorkerHeap=${mb(peaks.workerHeap)}MB` +
    (peaks.firstLoadMs !== undefined ? ` firstBundleLoad=${peaks.firstLoadMs}ms` : "") +
    "\n"
  );
}

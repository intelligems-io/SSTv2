import fs from "fs";
import path from "path";

// Configuration
const SST_BUILD_CONCURRENCY = parseInt(
  process.env.SST_BUILD_CONCURRENCY || "4",
  10
);
export const POOL_SIZE = parseInt(
  process.env.SST_WORKER_POOL_SIZE || "10",
  10
);
export const IDLE_TIMEOUT = parseInt(
  process.env.SST_WORKER_IDLE_TIMEOUT || "60000",
  10
);
const DEBUG_POOL = process.env.SST_DEBUG_POOL === "true";
const DEBUG_POOL_FILE =
  process.env.SST_DEBUG_POOL_FILE || ".sst/worker-pool.log";

// Bottleneck tracking metrics
interface PoolMetrics {
  coldStarts: Map<string, number>;
  concurrentRequests: Map<string, number>;
  peakConcurrent: Map<string, number>;
  totalRequests: Map<string, number>;
  poolHits: Map<string, number>;
  avgResponseTime: Map<string, number>;
  responseCount: Map<string, number>;
}

const metrics: PoolMetrics = {
  coldStarts: new Map(),
  concurrentRequests: new Map(),
  peakConcurrent: new Map(),
  totalRequests: new Map(),
  poolHits: new Map(),
  avgResponseTime: new Map(),
  responseCount: new Map(),
};

let logStream: fs.WriteStream | null = null;

// Function name resolver - set by workers.ts
let functionNameResolver: (functionID: string) => string = (id) =>
  id.slice(0, 25);

/**
 * Set the function name resolver callback
 * Called by workers.ts to provide access to useFunctions()
 */
export function setFunctionNameResolver(
  resolver: (functionID: string) => string
) {
  functionNameResolver = resolver;
}

function initLogFile() {
  if (!DEBUG_POOL || logStream) return;
  try {
    const logDir = path.dirname(DEBUG_POOL_FILE);
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }
    logStream = fs.createWriteStream(DEBUG_POOL_FILE, { flags: "a" });
    logStream.write(
      "\n" + "=".repeat(100) + "\n" +
        "[SESSION START] " + new Date().toISOString() + " | POOL_SIZE=" + POOL_SIZE + " IDLE_TIMEOUT=" + IDLE_TIMEOUT + "ms BUILD_CONCURRENCY=" + SST_BUILD_CONCURRENCY + "\n" +
        "=".repeat(100) + "\n"
    );
  } catch {
    // Fall back to no logging if file creation fails
  }
}

// Extract readable function name from handler path
function getFunctionName(functionID: string): string {
  return functionNameResolver(functionID);
}

// Calculate bottleneck indicators
function getBottleneckFlags(functionID: string): string {
  const flags: string[] = [];
  const concurrent = metrics.concurrentRequests.get(functionID) || 0;
  const total = metrics.totalRequests.get(functionID) || 0;
  const hits = metrics.poolHits.get(functionID) || 0;
  const hitRate = total > 0 ? (hits / total) * 100 : 0;
  const coldStarts = metrics.coldStarts.get(functionID) || 0;

  // SATURATED: All pool slots in use
  if (concurrent >= POOL_SIZE) {
    flags.push("🚨 SATURATED");
  }
  // HIGH_LOAD: >70% pool utilization
  else if (concurrent >= POOL_SIZE * 0.7) {
    flags.push("⚠️ HIGH_LOAD");
  }

  // COLD_START: Low hit rate indicates frequent cold starts
  if (total >= 5 && hitRate < 30) {
    flags.push("❄️ LOW_REUSE");
  }

  // BOTTLENECK: High cold start ratio
  if (total >= 3 && coldStarts / total > 0.5) {
    flags.push("🔥 COLD_HEAVY");
  }

  return flags.length > 0 ? " " + flags.join(" ") : "";
}

/**
 * Pool debug logging helper - writes to file with bottleneck detection
 */
export function logPool(action: string, details: Record<string, any> = {}) {
  if (!DEBUG_POOL) return;
  initLogFile();

  const timestamp = new Date().toISOString().slice(11, 23); // HH:MM:SS.mmm
  const funcName = details.functionID
    ? getFunctionName(details.functionID)
    : "";
  const bottleneckFlags =
    details.functionID &&
    ["CREATE", "REUSE", "POOL_MISS", "RESPONSE"].includes(action)
      ? getBottleneckFlags(details.functionID)
      : "";

  // Build metrics string for key actions
  let metricsStr = "";
  if (details.functionID && ["RESPONSE", "RETURN_TO_POOL"].includes(action)) {
    const concurrent = metrics.concurrentRequests.get(details.functionID) || 0;
    const total = metrics.totalRequests.get(details.functionID) || 0;
    const hits = metrics.poolHits.get(details.functionID) || 0;
    const hitRate = total > 0 ? ((hits / total) * 100).toFixed(0) : "0";
    metricsStr = " | concurrent=" + concurrent + " hitRate=" + hitRate + "%";
  }

  // Format details, replacing functionID with funcName
  const filteredDetails = { ...details };
  delete filteredDetails.functionID;
  const detailStr = Object.entries(filteredDetails)
    .map(([k, v]) => k + "=" + (typeof v === "object" ? JSON.stringify(v) : v))
    .join(" ");

  const actionPadded = action.padEnd(14);
  const funcPadded = funcName.padEnd(25);
  const line = "[" + timestamp + "] " + actionPadded + " " + funcPadded + " " + detailStr + metricsStr + bottleneckFlags + "\n";

  if (logStream) {
    logStream.write(line);
  }
}

/**
 * Track request lifecycle for metrics - called at request start
 */
export function trackRequestStart(functionID: string, isPoolHit: boolean) {
  const current = (metrics.concurrentRequests.get(functionID) || 0) + 1;
  metrics.concurrentRequests.set(functionID, current);
  metrics.totalRequests.set(
    functionID,
    (metrics.totalRequests.get(functionID) || 0) + 1
  );

  const peak = metrics.peakConcurrent.get(functionID) || 0;
  if (current > peak) {
    metrics.peakConcurrent.set(functionID, current);
  }

  if (isPoolHit) {
    metrics.poolHits.set(
      functionID,
      (metrics.poolHits.get(functionID) || 0) + 1
    );
  } else {
    metrics.coldStarts.set(
      functionID,
      (metrics.coldStarts.get(functionID) || 0) + 1
    );
  }
}

/**
 * Track request lifecycle for metrics - called at request end
 */
export function trackRequestEnd(functionID: string) {
  const current = metrics.concurrentRequests.get(functionID) || 0;
  if (current > 0) {
    metrics.concurrentRequests.set(functionID, current - 1);
  }
}

/**
 * Write session end summary and close log stream
 * Called from workers.ts on process exit
 */
export function writeSessionEndSummary() {
  if (!DEBUG_POOL || !logStream) return;

  let summary = "\n" + "─".repeat(100) + "\n[SESSION END] " + new Date().toISOString() + "\n";
  for (const [funcID, total] of metrics.totalRequests) {
    const hits = metrics.poolHits.get(funcID) || 0;
    const cold = metrics.coldStarts.get(funcID) || 0;
    const peak = metrics.peakConcurrent.get(funcID) || 0;
    const hitRate = total > 0 ? ((hits / total) * 100).toFixed(1) : "0";
    const funcName = getFunctionName(funcID);
    summary += "  " + funcName.padEnd(25) + " total=" + total + " poolHits=" + hits + " coldStarts=" + cold + " peakConcurrent=" + peak + " hitRate=" + hitRate + "%\n";
  }
  summary += "─".repeat(100) + "\n";
  logStream.write(summary);
  logStream.end();
}

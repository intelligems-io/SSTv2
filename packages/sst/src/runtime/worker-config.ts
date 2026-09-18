/**
 * Dev-mode worker knobs. Every value is overridable through the environment;
 * the defaults are sized so a laptop running `sst dev` stays responsive.
 */

function int(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const value = parseInt(raw, 10);
  return Number.isFinite(value) ? value : fallback;
}

/** Max live workers per pool key (one key for all mono-build Node functions). */
export const POOL_SIZE = int("SST_WORKER_POOL_SIZE", 4);

/** How long an idle worker is kept before it is terminated. */
export const IDLE_TIMEOUT = int("SST_WORKER_IDLE_TIMEOUT", 5 * 60 * 1000);

/** Invocations one Node worker may run at the same time. */
export const WORKER_CONCURRENCY = Math.max(1, int("SST_WORKER_CONCURRENCY", 10));

/** Workers to warm at dev start. 0 disables warmup. Capped at POOL_SIZE. */
export const WARMUP_COUNT = Math.min(POOL_SIZE, Math.max(0, int("SST_WARMUP_COUNT", 1)));

/** V8 old-space cap for each Node worker thread, in MB. 0 leaves it unbounded. */
export const WORKER_MAX_HEAP_MB = int("SST_WORKER_MAX_HEAP_MB", 1024);

/** Run Node workers with --enable-source-maps (costs memory per worker). */
export const SOURCE_MAPS = process.env.SST_SOURCE_MAPS === "true";

/** Sample process and worker memory to .sst/memory.log. */
export const DEBUG_MEMORY = process.env.SST_DEBUG_MEMORY === "true";

export const BUILD_CONCURRENCY = int("SST_BUILD_CONCURRENCY", 4);

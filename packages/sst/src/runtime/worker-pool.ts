/**
 * Bookkeeping for pooled dev workers.
 *
 * A pool key groups workers that can serve the same invocations: one key per
 * function normally, one shared key for every Node function in mono-build
 * mode. Each worker may hold up to `maxConcurrency` invocations at once, and a
 * key never holds more than `maxWorkers` live (non-stale) workers, so the
 * memory a key can consume is bounded by `maxWorkers` isolates.
 *
 * This module decides *which* worker serves a request and when a worker is
 * retired. It never starts or stops threads itself: `onTerminate` is the hook
 * the owner uses to actually stop the worker.
 */

export interface PoolWorker {
  id: string;
  poolKey: string;
  /** Function of the most recent invocation (shared pools serve many). */
  functionID: string;
  runtime: string;
  inFlight: number;
  maxConcurrency: number;
  /** A stale worker finishes what it holds, then terminates. */
  stale: boolean;
  createdAt: number;
  bundlePath: string;
  bundleMtime?: number;
  isSharedPool: boolean;
  idleTimer?: NodeJS.Timeout;
}

export type TerminateReason =
  | "idle_timeout"
  | "stale_mtime"
  | "rebuild"
  | "mono-rebuild"
  | "stale";

export interface WorkerPoolOptions {
  maxWorkers: number;
  idleTimeoutMs: number;
  onTerminate: (worker: PoolWorker, reason: TerminateReason) => void;
}

export interface PoolStats {
  workers: number;
  idle: number;
  inFlight: number;
}

export class WorkerPool {
  private readonly pools = new Map<string, PoolWorker[]>();
  private readonly byID = new Map<string, PoolWorker>();

  constructor(private readonly opts: WorkerPoolOptions) {}

  add(worker: PoolWorker) {
    let pool = this.pools.get(worker.poolKey);
    if (!pool) {
      pool = [];
      this.pools.set(worker.poolKey, pool);
    }
    pool.push(worker);
    this.byID.set(worker.id, worker);
  }

  get(id: string) {
    return this.byID.get(id);
  }

  workersFor(poolKey: string): readonly PoolWorker[] {
    return this.pools.get(poolKey) ?? [];
  }

  all(): PoolWorker[] {
    return [...this.byID.values()];
  }

  /** Live workers count toward the cap; stale ones are on their way out. */
  liveCount(poolKey: string) {
    return this.workersFor(poolKey).filter((w) => !w.stale).length;
  }

  canCreate(poolKey: string) {
    return this.liveCount(poolKey) < this.opts.maxWorkers;
  }

  /**
   * The busiest live worker that still has spare capacity, or undefined.
   * Packing invocations into as few workers as possible is what keeps memory
   * down: every worker that serves traffic grows to hold the handlers it has
   * loaded, so an idle spare is a few hundred MB doing nothing. Workers whose
   * bundle is older than `currentMtime` are retired on the way: idle ones
   * now, busy ones once they drain.
   */
  pick(poolKey: string, currentMtime?: number): PoolWorker | undefined {
    const pool = this.pools.get(poolKey);
    if (!pool || pool.length === 0) return undefined;

    for (const worker of [...pool]) {
      if (
        currentMtime &&
        worker.bundleMtime &&
        currentMtime > worker.bundleMtime &&
        !worker.stale
      ) {
        this.retire(worker, "stale_mtime");
      }
    }

    let best: PoolWorker | undefined;
    for (const worker of pool) {
      if (worker.stale) continue;
      if (worker.inFlight >= worker.maxConcurrency) continue;
      if (!best || worker.inFlight > best.inFlight) best = worker;
    }
    return best;
  }

  /** Hand an invocation to a worker. */
  checkout(worker: PoolWorker, functionID: string) {
    if (worker.idleTimer) {
      clearTimeout(worker.idleTimer);
      worker.idleTimer = undefined;
    }
    worker.inFlight += 1;
    worker.functionID = functionID;
  }

  /**
   * An invocation finished. Returns what became of the worker: it is gone
   * (stale and drained), idle (timer armed), or still busy.
   */
  checkin(worker: PoolWorker): "terminated" | "idle" | "busy" {
    if (worker.inFlight > 0) worker.inFlight -= 1;
    if (worker.inFlight > 0) return "busy";
    if (worker.stale) {
      this.remove(worker);
      this.opts.onTerminate(worker, "stale");
      return "terminated";
    }
    worker.idleTimer = setTimeout(() => {
      worker.idleTimer = undefined;
      if (worker.inFlight > 0) return;
      if (!this.byID.has(worker.id)) return;
      this.remove(worker);
      this.opts.onTerminate(worker, "idle_timeout");
    }, this.opts.idleTimeoutMs);
    return "idle";
  }

  /**
   * The code behind `poolKey` changed. Idle workers go now; busy ones are
   * marked stale and go when they drain. Returns the ids of workers that
   * were only marked.
   */
  invalidate(poolKey: string, reason: "rebuild" | "mono-rebuild"): string[] {
    const marked: string[] = [];
    for (const worker of [...this.workersFor(poolKey)]) {
      if (worker.inFlight === 0) {
        this.remove(worker);
        this.opts.onTerminate(worker, reason);
      } else {
        worker.stale = true;
        marked.push(worker.id);
      }
    }
    return marked;
  }

  /** Forget a worker that exited on its own (crash, OOM, self-timeout). */
  remove(worker: PoolWorker) {
    if (worker.idleTimer) {
      clearTimeout(worker.idleTimer);
      worker.idleTimer = undefined;
    }
    this.byID.delete(worker.id);
    const pool = this.pools.get(worker.poolKey);
    if (!pool) return;
    const idx = pool.indexOf(worker);
    if (idx >= 0) pool.splice(idx, 1);
    if (pool.length === 0) this.pools.delete(worker.poolKey);
  }

  stats(): PoolStats {
    let idle = 0;
    let inFlight = 0;
    for (const worker of this.byID.values()) {
      if (worker.inFlight === 0) idle += 1;
      inFlight += worker.inFlight;
    }
    return { workers: this.byID.size, idle, inFlight };
  }

  private retire(worker: PoolWorker, reason: TerminateReason) {
    if (worker.inFlight === 0) {
      this.remove(worker);
      this.opts.onTerminate(worker, reason);
    } else {
      worker.stale = true;
    }
  }
}

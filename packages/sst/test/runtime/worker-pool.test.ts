import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { PoolWorker, WorkerPool } from "../../src/runtime/worker-pool.js";

function worker(
  id: string,
  overrides: Partial<PoolWorker> = {}
): PoolWorker {
  return {
    id,
    poolKey: "nodejs:mono-build",
    functionID: "fn",
    runtime: "nodejs22.x",
    inFlight: 0,
    maxConcurrency: 2,
    stale: false,
    createdAt: Date.now(),
    bundlePath: "/app/.mono-build",
    bundleMtime: 100,
    isSharedPool: true,
    ...overrides,
  };
}

describe("WorkerPool", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("caps live workers per pool key", () => {
    const pool = new WorkerPool({ maxWorkers: 2, idleTimeoutMs: 1000, onTerminate: vi.fn() });
    expect(pool.canCreate("k")).toBe(true);
    pool.add(worker("a", { poolKey: "k" }));
    pool.add(worker("b", { poolKey: "k" }));
    expect(pool.canCreate("k")).toBe(false);
    expect(pool.canCreate("other")).toBe(true);
  });

  it("stale workers do not count toward the cap", () => {
    const pool = new WorkerPool({ maxWorkers: 1, idleTimeoutMs: 1000, onTerminate: vi.fn() });
    const a = worker("a", { inFlight: 1 });
    pool.add(a);
    pool.invalidate(a.poolKey, "mono-rebuild");
    expect(a.stale).toBe(true);
    expect(pool.canCreate(a.poolKey)).toBe(true);
  });

  it("packs into the busiest worker that still has capacity", () => {
    const pool = new WorkerPool({ maxWorkers: 5, idleTimeoutMs: 1000, onTerminate: vi.fn() });
    pool.add(worker("full", { inFlight: 2 }));
    pool.add(worker("busy", { inFlight: 1 }));
    pool.add(worker("idle", { inFlight: 0 }));
    expect(pool.pick("nodejs:mono-build")?.id).toBe("busy");
  });

  it("returns undefined when every worker is saturated", () => {
    const pool = new WorkerPool({ maxWorkers: 5, idleTimeoutMs: 1000, onTerminate: vi.fn() });
    pool.add(worker("a", { inFlight: 2 }));
    expect(pool.pick("nodejs:mono-build")).toBeUndefined();
  });

  it("retires idle workers built from an older bundle and marks busy ones stale", () => {
    const onTerminate = vi.fn();
    const pool = new WorkerPool({ maxWorkers: 5, idleTimeoutMs: 1000, onTerminate });
    const old = worker("old", { bundleMtime: 50 });
    const oldBusy = worker("oldBusy", { bundleMtime: 50, inFlight: 1 });
    const fresh = worker("fresh", { bundleMtime: 100 });
    pool.add(old);
    pool.add(oldBusy);
    pool.add(fresh);

    expect(pool.pick("nodejs:mono-build", 100)?.id).toBe("fresh");
    expect(onTerminate).toHaveBeenCalledWith(old, "stale_mtime");
    expect(pool.get("old")).toBeUndefined();
    expect(oldBusy.stale).toBe(true);
  });

  it("checkout clears the idle timer and checkin arms it", () => {
    const onTerminate = vi.fn();
    const pool = new WorkerPool({ maxWorkers: 5, idleTimeoutMs: 1000, onTerminate });
    const w = worker("a");
    pool.add(w);
    pool.checkout(w, "fn-1");
    expect(w.inFlight).toBe(1);
    expect(w.functionID).toBe("fn-1");
    expect(pool.checkin(w)).toBe("idle");
    pool.checkout(w, "fn-2");
    vi.advanceTimersByTime(5000);
    expect(onTerminate).not.toHaveBeenCalled();
    pool.checkin(w);
    vi.advanceTimersByTime(1000);
    expect(onTerminate).toHaveBeenCalledWith(w, "idle_timeout");
    expect(pool.get("a")).toBeUndefined();
  });

  it("checkin on a stale worker terminates it once it drains", () => {
    const onTerminate = vi.fn();
    const pool = new WorkerPool({ maxWorkers: 5, idleTimeoutMs: 1000, onTerminate });
    const w = worker("a", { inFlight: 2 });
    pool.add(w);
    pool.invalidate(w.poolKey, "rebuild");
    expect(pool.checkin(w)).toBe("busy");
    expect(onTerminate).not.toHaveBeenCalled();
    expect(pool.checkin(w)).toBe("terminated");
    expect(onTerminate).toHaveBeenCalledWith(w, "stale");
  });

  it("invalidate terminates idle workers immediately", () => {
    const onTerminate = vi.fn();
    const pool = new WorkerPool({ maxWorkers: 5, idleTimeoutMs: 1000, onTerminate });
    const idle = worker("idle");
    const busy = worker("busy", { inFlight: 1 });
    pool.add(idle);
    pool.add(busy);
    expect(pool.invalidate(idle.poolKey, "mono-rebuild")).toEqual(["busy"]);
    expect(onTerminate).toHaveBeenCalledWith(idle, "mono-rebuild");
    expect(pool.stats()).toEqual({ workers: 1, idle: 0, inFlight: 1 });
  });

  it("remove forgets a worker that exited on its own", () => {
    const onTerminate = vi.fn();
    const pool = new WorkerPool({ maxWorkers: 5, idleTimeoutMs: 1000, onTerminate });
    const w = worker("a");
    pool.add(w);
    pool.checkin(w);
    pool.remove(w);
    vi.advanceTimersByTime(2000);
    expect(onTerminate).not.toHaveBeenCalled();
    expect(pool.stats().workers).toBe(0);
  });
});

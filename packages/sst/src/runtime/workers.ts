import crypto from "crypto";
import fs from "fs";
import path from "path";
import { EventPayload, useBus } from "../bus.js";
import { RuntimeHandler, useFunctionBuilder, useRuntimeHandlers } from "./handlers.js";
import { useRuntimeServerConfig, useRuntimeServer } from "./server.js";
import { FunctionProps, useFunctions } from "../constructs/Function.js";
import { lazy } from "../util/lazy.js";
import { Logger } from "../logger.js";
import {
  logPool,
  logInvokeTrace,
  trackRequestStart,
  trackRequestEnd,
  setFunctionNameResolver,
  writeSessionEndSummary,
} from "./worker-pool-logging.js";
import {
  POOL_SIZE,
  IDLE_TIMEOUT,
  WORKER_CONCURRENCY,
  DEBUG_MEMORY,
} from "./worker-config.js";
import { PoolWorker, TerminateReason, WorkerPool } from "./worker-pool.js";
import { splitAttributed } from "./stdout-attribution.js";
import { startMemorySampling } from "./memory-logging.js";
import { useMonoBuildConfig, isMonoBuildPath } from "./mono-build-config.js";
import { getRequestPath, getCorrelationId, getApiGatewayRequestId } from "./request-utils.js";
import { logWorkers } from "./debug-bridge-logging.js";
import { logEventTrace } from "./event-trace-logging.js";

declare module "../bus.js" {
  export interface Events {
    "worker.started": {
      workerID: string;
      functionID: string;
    };
    "worker.stopped": {
      workerID: string;
      functionID: string;
    };
    "worker.exited": {
      workerID: string;
      functionID: string;
      /** Set for pooled workers: the id the runtime server keys on. */
      pooledWorkerID?: string;
    };
    "worker.stdout": {
      workerID: string;
      functionID: string;
      requestID: string;
      message: string;
    };
    "worker.reused": {
      workerID: string;
      functionID: string;
      pooledWorkerID: string;
    };
    "warmup.start": {
      count: number;
    };
    "warmup.progress": {
      completed: number;
      total: number;
      success: number;
      failed: number;
    };
    "warmup.complete": {
      success: number;
      failed: number;
      elapsedMs: number;
    };
  }
}

interface Worker {
  workerID: string;
  functionID: string;
}

/** One invocation currently held by a pooled worker. */
interface InFlightRequest {
  requestID: string;
  awsWorkerID: string;
  functionID: string;
  pooledWorkerID: string;
  startedAt: number;
}

/** An invocation that is waiting for pool capacity. */
interface PendingInvocation {
  evt: EventPayload<"function.invoked">;
  props: FunctionProps;
  handler: RuntimeHandler;
  build: { out: string; handler: string };
  poolKey: string;
  isShared: boolean;
  queuedAt: number;
}

const bundleMtimes = new Map<string, number>();
const bundleWatchers = new Map<string, fs.FSWatcher>();

// Clean up watchers on exit
process.on("exit", () => {
  for (const watcher of bundleWatchers.values()) {
    try {
      watcher.close();
    } catch {}
  }
});

// Get bundle rebuild timestamp for staleness checking
function getBundleMtime(buildOut: string): number | undefined {
  if (isMonoBuildPath(buildOut)) {
    if (bundleMtimes.has(buildOut)) {
      return bundleMtimes.get(buildOut);
    }

    const timestampFile = path.join(buildOut, ".last-rebuild");
    const update = () => {
      try {
        const content = fs.readFileSync(timestampFile, "utf-8");
        const mtime = parseInt(content, 10);
        bundleMtimes.set(buildOut, mtime);
        return mtime;
      } catch {
        return undefined;
      }
    };

    if (!bundleWatchers.has(buildOut)) {
      try {
        const watcher = fs.watch(buildOut, { persistent: false }, (event, filename) => {
          if (!filename || filename === ".last-rebuild") {
            update();
          }
        });
        watcher.on("error", () => {
          bundleWatchers.delete(buildOut);
          bundleMtimes.delete(buildOut);
          try { watcher.close(); } catch {}
        });
        bundleWatchers.set(buildOut, watcher);
      } catch {}
    }

    return update();
  }

  try {
    // For non-mono-bundle, use bundle directory mtime
    const stat = fs.statSync(buildOut);
    return stat.mtimeMs;
  } catch {
    return undefined;
  }
}

// Helper: Get pool key for worker lookup
// For mono-build, uses shared key so any warm worker can serve any handler
function getPoolKey(
  functionID: string,
  runtime: string,
  buildOut: string
): { key: string; isShared: boolean } {
  return useMonoBuildConfig().getPoolKey(functionID, runtime, buildOut);
}

// Extract readable function name from handler path
function getFunctionName(functionID: string): string {
  try {
    const props = useFunctions().fromID(functionID);
    if (!props) return functionID.slice(0, 25);
    return (props.functionName as string || functionID).split("backend-")[1]?.slice(0, 50) || functionID.slice(0, 25);
  } catch {
    return functionID.slice(0, 25);
  }
}

// Runtimes that support multiple invocations per process (have event loop)
const POOLABLE_RUNTIMES = new Set([
  // Node.js - has while(true) loop in nodejs-runtime/index.ts
  "nodejs",
  "nodejs14.x",
  "nodejs16.x",
  "nodejs18.x",
  "nodejs20.x",
  "nodejs22.x",
  "nodejs24.x",
  // Python - has while True loop in python-runtime/runtime.py
  "python",
  "python3.7",
  "python3.8",
  "python3.9",
  "python3.10",
  "python3.11",
  "python3.12",
  "python3.13",
  // Go - AWS Lambda Go SDK has built-in event loop
  "go",
  "go1.x",
  // Java - AWS Lambda Java SDK has built-in event loop
  "java",
  "java8",
  "java8.al2",
  "java11",
  "java17",
  "java21",
  // .NET - AWS Lambda .NET SDK has built-in event loop
  "dotnet",
  "dotnet6",
  "dotnet8",
  "dotnetcore3.1",
  // Rust - AWS Lambda Rust runtime has built-in event loop
  "rust",
]);

function isPoolableRuntime(
  runtime: string,
  env?: Record<string, any>
): boolean {
  // Container jobs are NOT poolable
  if (runtime.startsWith("container") && env?.SST_DEBUG_JOB) {
    return false;
  }
  return (
    POOLABLE_RUNTIMES.has(runtime) ||
    [...POOLABLE_RUNTIMES].some((r) => runtime.startsWith(r))
  );
}

/**
 * Resolve once `.mono-build/.last-rebuild` exists and has not changed for
 * `quietMs`. No-op when mono-build is off. Gives up after `maxWaitMs`.
 *
 * Dev start writes the timestamp twice about 7s apart (the app's initial
 * rebuild, then the watcher's first pass), so the quiet window has to be
 * longer than that gap or the warm worker is retired as stale right away.
 */
async function waitForStableBundle(quietMs = 10_000, maxWaitMs = 120_000) {
  const config = useMonoBuildConfig();
  const timestampFile = path.join(config.dir, ".last-rebuild");
  const started = Date.now();
  let last: string | undefined;
  let lastChange = Date.now();
  while (Date.now() - started < maxWaitMs) {
    if (!config.enabled) {
      // Not (yet) a mono-build project; nothing to wait for unless it appears
      if (Date.now() - started > quietMs) return;
    } else {
      let current: string | undefined;
      try {
        current = fs.readFileSync(timestampFile, "utf-8");
      } catch {}
      if (current !== last) {
        last = current;
        lastChange = Date.now();
      } else if (current !== undefined && Date.now() - lastChange >= quietMs) {
        return;
      }
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  logPool("WARMUP_BUNDLE_TIMEOUT", { waitedMs: Date.now() - started });
}

/** Only the Node runtime shim knows how to run invocations side by side. */
function concurrencyFor(runtime: string): number {
  return runtime.startsWith("nodejs") ? WORKER_CONCURRENCY : 1;
}

export const useRuntimeWorkers = lazy(async () => {
  // Set up function name resolver for logging module
  setFunctionNameResolver(getFunctionName);

  // Non-pooled workers (legacy behavior)
  const workers = new Map<string, Worker>();

  // Pooled workers and the invocations they hold
  const requests = new Map<string, InFlightRequest>(); // requestID → request
  const lastRequestId = new Map<string, string>(); // workerID → most recent requestID
  const waitQueues = new Map<string, PendingInvocation[]>(); // poolKey → waiting

  const bus = useBus();
  const handlers = useRuntimeHandlers();
  const builder = useFunctionBuilder();
  const serverConfig = await useRuntimeServerConfig();

  const pool = new WorkerPool({
    maxWorkers: POOL_SIZE,
    idleTimeoutMs: IDLE_TIMEOUT,
    onTerminate: (worker, reason) => {
      void stopPooledWorker(worker, reason);
    },
  });

  // Log pool configuration on startup
  logPool("INIT", {
    poolSize: POOL_SIZE,
    idleTimeoutMs: IDLE_TIMEOUT,
    concurrency: WORKER_CONCURRENCY,
    poolableRuntimes: [...POOLABLE_RUNTIMES].length,
  });

  // Lazy getter for server to avoid circular initialization
  let _server: Awaited<ReturnType<typeof useRuntimeServer>> | null = null;

  async function getServer() {
    if (!_server) {
      _server = await useRuntimeServer();
    }
    return _server;
  }

  function queuedCount(poolKey?: string) {
    if (poolKey) return waitQueues.get(poolKey)?.length ?? 0;
    let total = 0;
    for (const q of waitQueues.values()) total += q.length;
    return total;
  }

  // Helper: stop a pooled worker that the pool has already forgotten
  async function stopPooledWorker(worker: PoolWorker, reason: TerminateReason) {
    logPool("TERMINATE", {
      pooledWorkerID: worker.id.slice(0, 8),
      functionID: worker.functionID,
      reason,
      uptimeMs: Date.now() - worker.createdAt,
      inFlight: worker.inFlight,
    });

    try {
      const handler = handlers.for(worker.runtime);
      await handler?.stopWorker(worker.id);
    } catch (ex) {
      Logger.debug("Failed to stop pooled worker", worker.id, ex);
    }
    lastRequestId.delete(worker.id);
    Logger.debug("Terminated pooled worker", worker.id);

    // A slot opened up
    void drain(worker.poolKey);
  }

  // Helper: publish an error for every invocation a worker was holding
  function failRequestsOn(pooledWorkerID: string, errorType: string, errorMessage: string) {
    for (const req of [...requests.values()]) {
      if (req.pooledWorkerID !== pooledWorkerID) continue;
      requests.delete(req.requestID);
      trackRequestEnd(req.functionID);
      bus.publish("function.error", {
        workerID: req.awsWorkerID,
        functionID: req.functionID,
        requestID: req.requestID,
        errorType,
        errorMessage,
        trace: [],
      });
    }
  }

  // Helper: hand an invocation to a worker (new or reused)
  async function assign(worker: PoolWorker, pending: PendingInvocation, reused: boolean) {
    const { evt } = pending;
    const { workerID: awsWorkerID, functionID, requestID, event } = evt.properties;
    const requestPath = getRequestPath(event);

    pool.checkout(worker, functionID);
    requests.set(requestID, {
      requestID,
      awsWorkerID,
      functionID,
      pooledWorkerID: worker.id,
      startedAt: Date.now(),
    });
    lastRequestId.set(worker.id, requestID);
    trackRequestStart(functionID, reused);

    if (reused) {
      logPool("REUSE", {
        pooledWorkerID: worker.id.slice(0, 8),
        functionID,
        inFlight: worker.inFlight,
        workerAgeMs: Date.now() - worker.createdAt,
        waitedMs: Date.now() - pending.queuedAt,
      });
      logInvokeTrace("WORKER_REUSE", requestID, `pooled=${worker.id.slice(0, 8)}`);
      logEventTrace("WORKER_START", {
        requestID,
        functionID,
        workerID: worker.id,
        path: requestPath,
        correlationId: getCorrelationId(event),
        apiGwReqId: getApiGatewayRequestId(event),
        reused: true,
      });
      bus.publish("worker.reused", {
        workerID: awsWorkerID,
        functionID,
        pooledWorkerID: worker.id,
      });
    }

    const server = await getServer();
    logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Routing invocation to worker ${worker.id.slice(0, 8)} inFlight=${worker.inFlight}`);
    logInvokeTrace("ROUTE_INVOCATION", requestID);
    server.routeInvocation(worker.id, evt.properties);
  }

  // Helper: create a worker for an invocation. The worker is registered (and
  // counted against the cap) before the thread starts, so concurrent
  // dispatches cannot overshoot the pool size.
  async function createAndAssign(pending: PendingInvocation) {
    const { evt, props, handler, build, poolKey, isShared } = pending;
    const { workerID: awsWorkerID, functionID, requestID, env, event } = evt.properties;
    const requestPath = getRequestPath(event);

    const worker: PoolWorker = {
      id: crypto.randomBytes(16).toString("hex"),
      poolKey,
      functionID,
      runtime: props.runtime!,
      inFlight: 0,
      maxConcurrency: concurrencyFor(props.runtime!),
      stale: false,
      createdAt: Date.now(),
      bundlePath: build.out,
      bundleMtime: getBundleMtime(build.out),
      isSharedPool: isShared,
    };
    pool.add(worker);

    logPool("CREATE", {
      pooledWorkerID: worker.id.slice(0, 8),
      functionID,
      runtime: props.runtime,
      requestID: requestID.slice(0, 8),
      poolKey: poolKey.slice(0, 30),
      isSharedPool: isShared,
      liveWorkers: pool.liveCount(poolKey),
      waitedMs: Date.now() - pending.queuedAt,
    });

    // Route first so the invocation is queued at the server by the time the
    // worker asks for it.
    await assign(worker, pending, false);

    logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Starting worker ${worker.id.slice(0, 8)}...`);
    logInvokeTrace("WORKER_START", requestID, `pooled=${worker.id.slice(0, 8)}`);
    const workerStartTime = Date.now();
    try {
      await handler.startWorker({
        ...build,
        workerID: worker.id,
        functionID,
        environment: env,
        url: `${serverConfig.url}/${worker.id}/${serverConfig.API_VERSION}`,
        runtime: props.runtime!,
        isMonoBuild: isShared,
        concurrency: worker.maxConcurrency,
        debugMemory: DEBUG_MEMORY,
      });
    } catch (ex: any) {
      logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} ERROR: Failed to start worker: ${ex.message}`);
      Logger.debug("Failed to start pooled worker", ex);
      pool.remove(worker);
      failRequestsOn(worker.id, "WorkerStartFailed", `Failed to start pooled worker: ${ex.message}`);
      lastRequestId.delete(worker.id);
      void drain(poolKey);
      return;
    }

    const workerStartElapsed = Date.now() - workerStartTime;
    logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Worker started in ${workerStartElapsed}ms`);
    logInvokeTrace("WORKER_STARTED", requestID);
    logEventTrace("WORKER_START", {
      requestID,
      functionID,
      workerID: worker.id,
      path: requestPath,
      correlationId: getCorrelationId(event),
      apiGwReqId: getApiGatewayRequestId(event),
      elapsed: workerStartElapsed,
      reused: false,
    });
    bus.publish("worker.started", { workerID: awsWorkerID, functionID });
  }

  // Helper: place an invocation on a worker, create one, or wait for capacity
  async function dispatch(pending: PendingInvocation) {
    const { poolKey, build, evt } = pending;
    const { functionID, requestID } = evt.properties;
    const currentMtime = getBundleMtime(build.out);

    const worker = pool.pick(poolKey, currentMtime);
    if (worker) {
      await assign(worker, pending, true);
      return;
    }
    if (pool.canCreate(poolKey)) {
      await createAndAssign(pending);
      return;
    }

    let queue = waitQueues.get(poolKey);
    if (!queue) {
      queue = [];
      waitQueues.set(poolKey, queue);
    }
    queue.push(pending);
    logPool("POOL_WAIT", {
      functionID,
      requestID: requestID.slice(0, 8),
      poolKey: poolKey.slice(0, 30),
      queued: queue.length,
      liveWorkers: pool.liveCount(poolKey),
    });
    logInvokeTrace("POOL_WAIT", requestID, `queued=${queue.length}`);
  }

  // Helper: give waiting invocations to whatever capacity exists now
  async function drain(poolKey: string) {
    const queue = waitQueues.get(poolKey);
    if (!queue || queue.length === 0) return;
    while (queue.length > 0) {
      const head = queue[0];
      const currentMtime = getBundleMtime(head.build.out);
      const worker = pool.pick(poolKey, currentMtime);
      if (worker) {
        queue.shift();
        await assign(worker, head, true);
        continue;
      }
      if (pool.canCreate(poolKey)) {
        queue.shift();
        await createAndAssign(head);
        continue;
      }
      break;
    }
    if (queue.length === 0) waitQueues.delete(poolKey);
  }

  // Helper: an invocation finished on a pooled worker
  function release(pooledWorkerID: string, requestID: string | undefined) {
    const req = requestID ? requests.get(requestID) : undefined;
    if (req) {
      requests.delete(req.requestID);
      trackRequestEnd(req.functionID);
    }
    const worker = pool.get(pooledWorkerID);
    if (!worker) return;

    logPool("RESPONSE", {
      pooledWorkerID: pooledWorkerID.slice(0, 8),
      functionID: req?.functionID ?? worker.functionID,
      requestID: requestID?.slice(0, 8),
      inFlight: worker.inFlight - 1,
      durationMs: req ? Date.now() - req.startedAt : undefined,
    });

    const outcome = pool.checkin(worker);
    if (outcome === "terminated") return; // stopPooledWorker drains
    if (outcome === "idle") {
      logPool("IDLE", {
        pooledWorkerID: pooledWorkerID.slice(0, 8),
        functionID: worker.functionID,
        poolKey: worker.poolKey.slice(0, 30),
        idleTimeoutMs: IDLE_TIMEOUT,
      });
    }
    void drain(worker.poolKey);
  }

  // Build success handler - retire workers running the old code
  handlers.subscribe("function.build.success", async (evt) => {
    const { functionID } = evt.properties;
    const props = useFunctions().fromID(functionID);
    if (!props) return;

    const build = await builder.artifact(functionID);
    const isMonoBuild = build ? isMonoBuildPath(build.out) : false;
    const poolKey = isMonoBuild ? `${props.runtime}:mono-build` : `${props.runtime}:${functionID}`;
    const before = pool.workersFor(poolKey).length;
    const marked = pool.invalidate(poolKey, isMonoBuild ? "mono-rebuild" : "rebuild");

    logPool(isMonoBuild ? "MONO_BUILD_CLEAR" : "BUILD_CLEAR", {
      functionID,
      poolKey,
      pooledWorkersCleared: before - marked.length,
      activeWorkersMarkedStale: marked.length,
    });

    // Stop non-pooled workers (legacy behavior)
    for (const [_, worker] of workers) {
      if (worker.functionID === functionID) {
        const workerProps = useFunctions().fromID(worker.functionID);
        if (!workerProps) return;
        const handler = handlers.for(workerProps.runtime!);
        await handler?.stopWorker(worker.workerID);
        bus.publish("worker.stopped", worker);
      }
    }
  });

  // Main invocation handler
  bus.subscribe("function.invoked", async (evt) => {
    const {
      workerID: awsWorkerID,
      functionID,
      requestID,
      env,
      event,
    } = evt.properties;

    const startTime = Date.now();
    const requestPath = getRequestPath(event);

    // Warm pings ({ding}/{warmer}) are ordinary invocations here: they take
    // a pooled worker if one is free and wait for capacity otherwise, so a
    // burst of them can never hold more isolates than the pool allows.
    const isWarmupRequest = event && typeof event === 'object' &&
      ('ding' in (event as any) || 'warmer' in (event as any) || (event as any).__sst_warmup === true);

    logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} RECEIVED func=${functionID.slice(-30)}`);
    logInvokeTrace(isWarmupRequest ? "WARMUP_RECEIVED" : "INVOKE_RECEIVED", requestID, `func=${functionID.slice(-40)}`);

    // Send ack immediately
    bus.publish("function.ack", { functionID, workerID: awsWorkerID, requestID });
    logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} ACK sent elapsed=${Date.now() - startTime}ms`);
    logInvokeTrace("ACK_PUBLISHED", requestID, `elapsed=${Date.now() - startTime}ms`);

    const props = useFunctions().fromID(functionID);
    if (!props) {
      logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} ERROR: Function not found`);
      Logger.debug("Function not found:", functionID);
      bus.publish("function.error", {
        workerID: awsWorkerID,
        functionID,
        requestID,
        errorType: "FunctionNotFound",
        errorMessage: `Function ${functionID} not found in project`,
        trace: [],
      });
      return;
    }

    const handler = handlers.for(props.runtime!);
    if (!handler) {
      logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} ERROR: No handler for runtime ${props.runtime}`);
      Logger.debug("No handler for runtime:", props.runtime);
      bus.publish("function.error", {
        workerID: awsWorkerID,
        functionID,
        requestID,
        errorType: "RuntimeNotSupported",
        errorMessage: `No handler for runtime ${props.runtime}`,
        trace: [],
      });
      return;
    }

    logInvokeTrace("BUILD_ARTIFACT_START", requestID);
    const buildStartTime = Date.now();
    const build = await builder.artifact(functionID);
    logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Build artifact took ${Date.now() - buildStartTime}ms`);
    logInvokeTrace("BUILD_ARTIFACT_DONE", requestID, build ? `out=${build.out.slice(-30)}` : "NO_BUILD");
    if (!build) {
      logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} ERROR: Build artifact not ready`);
      Logger.debug("Build artifact not ready for:", functionID);
      bus.publish("function.error", {
        workerID: awsWorkerID,
        functionID,
        requestID,
        errorType: "BuildFailed",
        errorMessage: `Build artifact not available for ${functionID}. Check for build errors.`,
        trace: [],
      });
      return;
    }

    if (isPoolableRuntime(props.runtime!, env)) {
      // === POOLED PATH ===
      const { key: poolKey, isShared } = getPoolKey(functionID, props.runtime!, build.out);
      logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Dispatching to pool ${poolKey.slice(0, 20)}`);
      await dispatch({
        evt,
        props,
        handler,
        build,
        poolKey,
        isShared,
        queuedAt: Date.now(),
      });
      return;
    }

    // === NON-POOLED PATH (existing behavior) ===
    lastRequestId.set(awsWorkerID, requestID);

    let worker = workers.get(awsWorkerID);
    if (worker) return;

    try {
      await handler.startWorker({
        ...build,
        workerID: awsWorkerID,
        functionID,
        environment: env,
        url: `${serverConfig.url}/${awsWorkerID}/${serverConfig.API_VERSION}`,
        runtime: props.runtime!,
        isMonoBuild: isMonoBuildPath(build.out),
      });

      workers.set(awsWorkerID, { workerID: awsWorkerID, functionID });
      bus.publish("worker.started", { workerID: awsWorkerID, functionID });

      const server = await getServer();
      server.routeInvocation(awsWorkerID, evt.properties);
    } catch (ex: any) {
      Logger.debug("Failed to start worker", ex);
      bus.publish("function.error", {
        workerID: awsWorkerID,
        functionID,
        requestID,
        errorType: "WorkerStartFailed",
        errorMessage: `Failed to start worker: ${ex.message}`,
        trace: ex.stack?.split("\n") || [],
      });
      return;
    }
  });

  const stats = () => {
    const s = pool.stats();
    return { workers: s.workers, inFlight: s.inFlight, queued: queuedCount() };
  };
  startMemorySampling(stats);

  // Process exit cleanup
  process.on("exit", () => {
    writeSessionEndSummary();
    for (const worker of pool.all()) {
      if (worker.idleTimer) clearTimeout(worker.idleTimer);
    }
  });

  return {
    fromID(workerID: string) {
      const pooled = pool.get(workerID);
      if (pooled) return { workerID, functionID: pooled.functionID };
      return workers.get(workerID)!;
    },

    getCurrentRequestID(workerID: string) {
      return lastRequestId.get(workerID);
    },

    /**
     * Who a response belongs to. Pooled workers may hold several requests,
     * so the request id decides; non-pooled workers are their own AWS worker.
     */
    resolveRequest(workerID: string, requestID?: string) {
      const req = requestID ? requests.get(requestID) : undefined;
      if (req) return { awsWorkerID: req.awsWorkerID, functionID: req.functionID };
      const pooled = pool.get(workerID);
      if (pooled) {
        const last = lastRequestId.get(workerID);
        const lastReq = last ? requests.get(last) : undefined;
        return {
          awsWorkerID: lastReq?.awsWorkerID ?? workerID,
          functionID: lastReq?.functionID ?? pooled.functionID,
        };
      }
      const worker = workers.get(workerID);
      if (!worker) return undefined;
      return { awsWorkerID: workerID, functionID: worker.functionID };
    },

    stdout(workerID: string, message: string) {
      const pooled = pool.get(workerID);
      if (pooled) {
        for (const chunk of splitAttributed(message)) {
          const requestID = chunk.requestID ?? lastRequestId.get(workerID);
          if (!requestID) continue;
          const trimmed = chunk.text.trim();
          if (!trimmed) continue;
          const functionID = requests.get(requestID)?.functionID ?? pooled.functionID;
          if (trimmed.includes("[LOG]")) {
            logEventTrace("WORKER_LOG", { requestID, functionID, workerID, message: trimmed });
          }
          bus.publish("worker.stdout", { workerID, functionID, message: trimmed, requestID });
        }
        return;
      }

      // Non-pooled worker
      const worker = workers.get(workerID);
      if (!worker) return;

      const trimmedMessage = message.trim();
      const requestID = lastRequestId.get(workerID);
      if (trimmedMessage.includes("[LOG]") && requestID) {
        logEventTrace("WORKER_LOG", {
          requestID,
          functionID: worker.functionID,
          workerID,
          message: trimmedMessage,
        });
      }

      bus.publish("worker.stdout", {
        ...worker,
        message: trimmedMessage,
        requestID: requestID!,
      });
    },

    exited(workerID: string) {
      const pooled = pool.get(workerID);
      if (pooled) {
        logPool("EXIT", {
          pooledWorkerID: workerID.slice(0, 8),
          functionID: pooled.functionID,
          inFlight: pooled.inFlight,
          uptimeMs: Date.now() - pooled.createdAt,
        });
        pool.remove(pooled);
        const last = lastRequestId.get(workerID);
        const awsWorkerID = last ? requests.get(last)?.awsWorkerID : undefined;
        // Anything it was holding will never get a response from it
        failRequestsOn(
          workerID,
          "WorkerExited",
          "Local worker exited before responding (out of memory or crashed). Check the dev console for details."
        );
        lastRequestId.delete(workerID);
        bus.publish("worker.exited", {
          workerID: awsWorkerID ?? workerID,
          functionID: pooled.functionID,
          pooledWorkerID: workerID,
        });
        void drain(pooled.poolKey);
        return;
      }

      // Non-pooled worker
      const existing = workers.get(workerID);
      if (!existing) return;
      workers.delete(workerID);
      lastRequestId.delete(workerID);
      bus.publish("worker.exited", existing);
    },

    // Called by server when a response or error is received
    onResponse(pooledWorkerID: string, requestID?: string) {
      if (!pool.get(pooledWorkerID)) return;
      release(pooledWorkerID, requestID ?? lastRequestId.get(pooledWorkerID));
    },

    // Check if worker is pooled
    isPooled(workerID: string): boolean {
      return pool.get(workerID) !== undefined;
    },

    stats,

    subscribe: bus.forward(
      "worker.started",
      "worker.stopped",
      "worker.exited",
      "worker.stdout",
      "worker.reused"
    ),

    /**
     * Warm `workersToWarm` pooled workers by invoking a Node function with
     * concurrent warm pings.
     *
     * Each ping is marked as a fan-out *child* (`__WARMER_INVOCATION__ > 1`)
     * so the app's lambda-warmer preloads its handlers and returns instead of
     * fanning out to `concurrency` more Lambdas, which is what turned the old
     * 30 pings into ~900 worker creations. Pings go through the normal pool
     * path, so warmup can never hold more isolates than steady state.
     *
     * In mono-build mode this waits for the bundle to settle first: dev start
     * kicks off the bundle's first watch build, and a worker loaded before it
     * lands is retired as stale seconds later, which wasted every warm worker.
     */
    async triggerWarmup(workersToWarm: number) {
      workersToWarm = Math.min(workersToWarm, POOL_SIZE);
      if (workersToWarm <= 0) return { warmed: 0 };
      const count = workersToWarm * WORKER_CONCURRENCY;

      await waitForStableBundle();

      const functions = useFunctions();
      const allFunctions = functions.all;

      // Find a nodejs function to use as the warmup target
      let targetFunction: { id: string; props: any; functionName: string } | null = null;

      for (const [id, props] of Object.entries(allFunctions)) {
        if (!props.runtime?.startsWith("nodejs")) continue;
        if (!isPoolableRuntime(props.runtime!)) continue;
        if (!props.functionName) continue;

        targetFunction = { id, props, functionName: props.functionName as string };
        break;
      }

      if (!targetFunction) {
        logPool("WARMUP_SKIP", { reason: "no nodejs function found" });
        return { warmed: 0 };
      }

      const { functionName } = targetFunction;

      logPool("WARMUP_START", { count, workersToWarm, functionName });
      bus.publish("warmup.start", { count });

      const startTime = Date.now();
      let success = 0;
      let failed = 0;
      let completed = 0;

      const { useAWSClient } = await import("../credentials.js");
      const { LambdaClient, InvokeCommand } = await import("@aws-sdk/client-lambda");
      const lambda = useAWSClient(LambdaClient);

      const publishProgress = () => {
        bus.publish("warmup.progress", { completed, total: count, success, failed });
      };

      await Promise.all(
        Array.from({ length: count }, (_, i) => i).map(async (i) => {
          try {
            const result = await lambda.send(
              new InvokeCommand({
                FunctionName: functionName,
                InvocationType: "RequestResponse",
                Payload: JSON.stringify({
                  ding: true,
                  __WARMER_INVOCATION__: i + 2,
                  __WARMER_CONCURRENCY__: count + 1,
                  __WARMER_CORRELATIONID__: `sst-dev-warmup-${startTime}`,
                }),
              })
            );
            if (result.StatusCode === 200) success++;
            else failed++;
          } catch {
            failed++;
          }
          completed++;
          publishProgress();
        })
      );

      const elapsed = Date.now() - startTime;
      logPool("WARMUP_DONE", {
        success,
        failed,
        elapsedMs: elapsed,
        avgMs: success > 0 ? Math.round(elapsed / success) : 0,
      });
      bus.publish("warmup.complete", { success, failed, elapsedMs: elapsed });

      return { warmed: success, elapsed };
    },
  };
});

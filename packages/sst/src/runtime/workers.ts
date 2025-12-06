import crypto from "crypto";
import fs from "fs";
import path from "path";
import {useBus} from "../bus.js";
import {useFunctionBuilder, useRuntimeHandlers} from "./handlers.js";
import {useRuntimeServerConfig, useRuntimeServer} from "./server.js";
import {useFunctions} from "../constructs/Function.js";
import {lazy} from "../util/lazy.js";
import {Logger} from "../logger.js";
import {
  POOL_SIZE,
  IDLE_TIMEOUT,
  logPool,
  logInvokeTrace,
  trackRequestStart,
  trackRequestEnd,
  setFunctionNameResolver,
  writeSessionEndSummary,
} from "./worker-pool-logging.js";
import {useMonoBuildConfig, isMonoBuildPath} from "./mono-build-config.js";
import {getRequestPath} from "./request-utils.js";
import {logWorkers} from "./debug-bridge-logging.js";

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

interface PooledWorker {
  pooledWorkerID: string;
  functionID: string;
  state: "idle" | "busy";
  idleTimer?: NodeJS.Timeout;
  createdAt: number;
  poolKey: string;        // Pool lookup key (shared for mono-build)
  isSharedPool: boolean;  // Whether using shared pool (mono-build mode)
  bundlePath?: string;    // Path to bundle file (for mtime checking)
  bundleMtime?: number;   // Bundle mtime when worker was created
}

// Track workers marked as stale (should not return to pool after completion)
const staleWorkers = new Set<string>();

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
  // Use the global mono build config for pool key calculation
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

export const useRuntimeWorkers = lazy(async () => {
  // Set up function name resolver for logging module
  setFunctionNameResolver(getFunctionName);

  // Non-pooled workers (legacy behavior)
  const workers = new Map<string, Worker>();

  // Worker pool data structures
  const workerPool = new Map<string, PooledWorker[]>();
  const activeWorkers = new Map<string, PooledWorker>();
  const workerIDMapping = new Map<string, string>(); // awsWorkerID → pooledWorkerID
  const reverseMapping = new Map<string, string>(); // pooledWorkerID → awsWorkerID
  const startedWorkers = new Set<string>(); // Track started pooledWorkerIDs

  const bus = useBus();
  const handlers = useRuntimeHandlers();
  const builder = useFunctionBuilder();
  const serverConfig = await useRuntimeServerConfig();

  // Log pool configuration on startup
  logPool("INIT", {
    poolSize: POOL_SIZE,
    idleTimeoutMs: IDLE_TIMEOUT,
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

  // Helper: Terminate a pooled worker
  async function terminatePooledWorker(
    pooledWorkerID: string,
    reason?: string
  ) {
    const worker =
      activeWorkers.get(pooledWorkerID) ||
      [...workerPool.values()]
        .flat()
        .find((w) => w.pooledWorkerID === pooledWorkerID);
    if (!worker) return;

    const props = useFunctions().fromID(worker.functionID);
    if (!props) return;

    const uptime = Date.now() - worker.createdAt;
    logPool("TERMINATE", {
      pooledWorkerID: pooledWorkerID.slice(0, 8),
      functionID: worker.functionID,
      reason: reason || "unknown",
      uptimeMs: uptime,
    });

    const handler = handlers.for(props.runtime!);
    await handler?.stopWorker(pooledWorkerID);

    // Clean up mappings
    activeWorkers.delete(pooledWorkerID);
    staleWorkers.delete(pooledWorkerID);
    const awsWorkerID = reverseMapping.get(pooledWorkerID);
    if (awsWorkerID) {
      workerIDMapping.delete(awsWorkerID);
    }
    reverseMapping.delete(pooledWorkerID);
    startedWorkers.delete(pooledWorkerID);
    lastRequestId.delete(pooledWorkerID);

    Logger.debug("Terminated pooled worker", pooledWorkerID);
  }

  // Helper: Get idle worker from pool
  // Uses poolKey for lookup (shared key for mono-build)
  function getIdleWorker(
    poolKey: string,
    functionID: string,
    buildOut: string
  ): PooledWorker | undefined {
    const pool = workerPool.get(poolKey);
    if (!pool || pool.length === 0) {
      logPool("POOL_MISS", {
        functionID,
        poolKey: poolKey.slice(0, 30),
        poolSize: 0,
      });
      return undefined;
    }

    // Check current bundle mtime for staleness detection
    const currentMtime = getBundleMtime(buildOut);

    // Try to find a non-stale worker
    while (pool.length > 0) {
      const worker = pool.pop();
      if (!worker) break;

      clearTimeout(worker.idleTimer);

      // Check if worker is stale (bundle was modified since worker started)
      if (currentMtime && worker.bundleMtime && currentMtime > worker.bundleMtime) {
        logPool("STALE_MTIME", {
          pooledWorkerID: worker.pooledWorkerID.slice(0, 8),
          functionID,
          workerMtime: worker.bundleMtime,
          currentMtime,
        });
        terminatePooledWorker(worker.pooledWorkerID, "stale_mtime");
        continue; // Try next worker
      }

      worker.state = "busy";
      const age = Date.now() - worker.createdAt;
      const crossFunction = worker.functionID !== functionID;
      logPool("REUSE", {
        pooledWorkerID: worker.pooledWorkerID.slice(0, 8),
        functionID,
        originalFunctionID: crossFunction ? worker.functionID : undefined,
        poolSizeAfter: pool.length,
        workerAgeMs: age,
        crossFunction,
      });
      Logger.debug(
        "Reusing pooled worker",
        worker.pooledWorkerID,
        "for",
        functionID,
        crossFunction ? "(cross-function reuse)" : ""
      );
      return worker;
    }

    // All workers were stale
    logPool("POOL_MISS", {
      functionID,
      poolKey: poolKey.slice(0, 30),
      poolSize: 0,
      reason: "all_stale",
    });
    return undefined;
  }

  // Helper: Return worker to pool
  // Uses poolKey for pool lookup (shared key for mono-build)
  function returnToPool(pooledWorkerID: string) {
    const worker = activeWorkers.get(pooledWorkerID);
    if (!worker) return;

    // Check if worker is stale (marked for termination due to rebuild)
    if (staleWorkers.has(pooledWorkerID)) {
      staleWorkers.delete(pooledWorkerID);
      logPool("STALE_TERMINATE", {
        pooledWorkerID: pooledWorkerID.slice(0, 8),
        functionID: worker.functionID,
        reason: "marked-stale-during-rebuild",
      });
      terminatePooledWorker(pooledWorkerID, "stale");
      return;
    }

    // Clean up current request mappings
    const awsWorkerID = reverseMapping.get(pooledWorkerID);
    if (awsWorkerID) {
      workerIDMapping.delete(awsWorkerID);
      reverseMapping.delete(pooledWorkerID);
    }

    // Use poolKey for pool lookup (shared for mono-build)
    let pool = workerPool.get(worker.poolKey);
    if (!pool) {
      pool = [];
      workerPool.set(worker.poolKey, pool);
    }

    if (pool.length >= POOL_SIZE) {
      // Pool full, terminate
      logPool("POOL_FULL", {
        pooledWorkerID: pooledWorkerID.slice(0, 8),
        functionID: worker.functionID,
        poolKey: worker.poolKey.slice(0, 30),
        poolSize: pool.length,
        maxSize: POOL_SIZE,
      });
      terminatePooledWorker(pooledWorkerID, "pool_full");
      Logger.debug("Pool full, terminated worker", pooledWorkerID);
      return;
    }

    // Return to pool with idle timeout
    worker.state = "idle";
    worker.idleTimer = setTimeout(() => {
      const idx = pool!.indexOf(worker);
      if (idx >= 0) pool!.splice(idx, 1);
      terminatePooledWorker(pooledWorkerID, "idle_timeout");
      Logger.debug("Idle timeout, terminated worker", pooledWorkerID);
    }, IDLE_TIMEOUT);

    pool.push(worker);
    activeWorkers.delete(pooledWorkerID);
    logPool("RETURN_TO_POOL", {
      pooledWorkerID: pooledWorkerID.slice(0, 8),
      functionID: worker.functionID,
      poolKey: worker.poolKey.slice(0, 30),
      isSharedPool: worker.isSharedPool,
      poolSizeAfter: pool.length,
      idleTimeoutMs: IDLE_TIMEOUT,
    });
    Logger.debug(
      "Returned worker to pool",
      pooledWorkerID,
      "pool key:",
      worker.poolKey,
      "pool size:",
      pool.length
    );
  }

  // Build success handler - clear pool for rebuilt function
  handlers.subscribe("function.build.success", async (evt) => {
    const {functionID} = evt.properties;
    const props = useFunctions().fromID(functionID);
    if (!props) return;

    // Get build to check if mono-build using global config
    const build = await builder.artifact(functionID);
    const isMonoBuild = build ? isMonoBuildPath(build.out) : false;

    if (isMonoBuild) {
      // For mono-build: clear the entire shared pool since all functions share the same bundle
      const sharedPoolKey = `${props.runtime}:mono-build`;
      const sharedPool = workerPool.get(sharedPoolKey) || [];
      const activeSharedCount = [...activeWorkers.values()].filter(
        (w) => w.isSharedPool && w.poolKey === sharedPoolKey
      ).length;

      logPool("MONO_BUILD_CLEAR", {
        functionID,
        sharedPoolKey,
        pooledWorkersCleared: sharedPool.length,
        activeWorkersMarkedStale: activeSharedCount,
      });

      // Terminate all idle workers in the shared pool
      for (const worker of sharedPool) {
        clearTimeout(worker.idleTimer);
        await terminatePooledWorker(worker.pooledWorkerID, "mono-rebuild");
      }
      workerPool.delete(sharedPoolKey);

      // Mark active workers as stale (they'll be terminated after completing their request)
      for (const [pooledID, worker] of activeWorkers) {
        if (worker.isSharedPool && worker.poolKey === sharedPoolKey) {
          staleWorkers.add(pooledID);
          logPool("MARK_STALE", {
            pooledWorkerID: pooledID.slice(0, 8),
            functionID: worker.functionID,
            reason: "mono-rebuild",
          });
        }
      }
    } else {
      // For non-mono-build: clear pool for this specific function only
      const pool = workerPool.get(`${props.runtime}:${functionID}`) || [];
      const activeCount = [...activeWorkers.values()].filter(
        (w) => w.functionID === functionID
      ).length;

      logPool("BUILD_CLEAR", {
        functionID,
        pooledWorkersCleared: pool.length,
        activeWorkersMarkedStale: activeCount,
      });

      for (const worker of pool) {
        clearTimeout(worker.idleTimer);
        await terminatePooledWorker(worker.pooledWorkerID, "rebuild");
      }
      workerPool.delete(`${props.runtime}:${functionID}`);

      // Mark active workers as stale (they'll be terminated after completing their request)
      for (const [pooledID, worker] of activeWorkers) {
        if (worker.functionID === functionID) {
          staleWorkers.add(pooledID);
          logPool("MARK_STALE", {
            pooledWorkerID: pooledID.slice(0, 8),
            functionID: worker.functionID,
            reason: "rebuild",
          });
        }
      }
    }

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

  const lastRequestId = new Map<string, string>();

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

    // Check if this is a warmup request - force-create new workers for these
    // Matches the warmer format: { ding: true } or { warmer: true }
    const isWarmupRequest = event && typeof event === 'object' &&
      ('ding' in (event as any) || 'warmer' in (event as any) || (event as any).__sst_warmup === true);
    const warmupId = isWarmupRequest ? ((event as any).warmupId ?? (event as any).index) : undefined;

    logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} RECEIVED func=${functionID.slice(-30)}`);

    if (isWarmupRequest) {
      logInvokeTrace("WARMUP_RECEIVED", requestID, `warmupId=${warmupId}`);
    } else {
      logInvokeTrace("INVOKE_RECEIVED", requestID, `func=${functionID.slice(-40)}`);
    }

    // Send ack immediately
    bus.publish("function.ack", {functionID, workerID: awsWorkerID, requestID});
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

    logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Getting build artifact...`);
    logInvokeTrace("BUILD_ARTIFACT_START", requestID);
    const buildStartTime = Date.now();
    const build = await builder.artifact(functionID);
    const buildElapsed = Date.now() - buildStartTime;
    logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Build artifact took ${buildElapsed}ms`);
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

    // Check if this runtime supports pooling
    const poolable = isPoolableRuntime(props.runtime!, env);

    if (poolable) {
      // === POOLED PATH ===
      // Get pool key: shared for mono-build, per-function otherwise
      const { key: poolKey, isShared } = getPoolKey(
        functionID,
        props.runtime!,
        build.out
      );

      logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Looking for pooled worker, poolKey=${poolKey.slice(0, 20)}`);

      // For warmup requests, always create new workers (never reuse from pool)
      let pooledWorker = isWarmupRequest ? undefined : getIdleWorker(poolKey, functionID, build.out);
      let isReuse = false;

      if (pooledWorker) {
        isReuse = true;
        // Update functionID for cross-function reuse (mono-build)
        pooledWorker.functionID = functionID;
        trackRequestStart(functionID, true);
        logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} REUSING pooled worker ${pooledWorker.pooledWorkerID.slice(0, 8)}`);
      } else {
        // Create new pooled worker
        const pooledWorkerID = crypto.randomBytes(16).toString("hex");
        const bundleMtime = getBundleMtime(build.out);
        pooledWorker = {
          pooledWorkerID,
          functionID,
          state: "busy",
          createdAt: Date.now(),
          poolKey,
          isSharedPool: isShared,
          bundlePath: build.out,
          bundleMtime,
        };
        logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} CREATING new pooled worker ${pooledWorkerID.slice(0, 8)}`);
      }

      // Set up mappings
      workerIDMapping.set(awsWorkerID, pooledWorker.pooledWorkerID);
      reverseMapping.set(pooledWorker.pooledWorkerID, awsWorkerID);
      lastRequestId.set(pooledWorker.pooledWorkerID, requestID);
      activeWorkers.set(pooledWorker.pooledWorkerID, pooledWorker);

      if (!isReuse) {
        // Start new worker with pooledWorkerID (cold start)
        trackRequestStart(functionID, false);
        const currentPoolSize = workerPool.get(poolKey)?.length || 0;
        logPool(isWarmupRequest ? "WARMUP_CREATE" : "CREATE", {
          pooledWorkerID: pooledWorker.pooledWorkerID.slice(0, 8),
          functionID,
          runtime: props.runtime,
          requestID: requestID.slice(0, 8),
          poolKey: poolKey.slice(0, 30),
          isSharedPool: isShared,
          currentPoolSize,
          activeWorkers: activeWorkers.size,
          ...(isWarmupRequest && { warmupId }),
        });

        logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Starting worker ${pooledWorker.pooledWorkerID.slice(0, 8)}...`);
        logInvokeTrace("WORKER_START", requestID, `pooled=${pooledWorker.pooledWorkerID.slice(0, 8)}`);
        const workerStartTime = Date.now();
        try {
          await handler.startWorker({
            ...build,
            workerID: pooledWorker.pooledWorkerID,
            functionID,
            environment: env,
            url: `${serverConfig.url}/${pooledWorker.pooledWorkerID}/${serverConfig.API_VERSION}`,
            runtime: props.runtime!,
            isMonoBuild: isShared,
          });
          startedWorkers.add(pooledWorker.pooledWorkerID);
          logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Worker started in ${Date.now() - workerStartTime}ms`);
          logInvokeTrace("WORKER_STARTED", requestID);

          bus.publish("worker.started", {
            workerID: awsWorkerID,
            functionID,
          });
        } catch (ex: any) {
          logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} ERROR: Failed to start worker: ${ex.message}`);
          Logger.debug("Failed to start pooled worker", ex);
          bus.publish("function.error", {
            workerID: awsWorkerID,
            functionID,
            requestID,
            errorType: "WorkerStartFailed",
            errorMessage: `Failed to start pooled worker: ${ex.message}`,
            trace: ex.stack?.split("\n") || [],
          });
          // Cleanup failed worker state
          activeWorkers.delete(pooledWorker.pooledWorkerID);
          startedWorkers.delete(pooledWorker.pooledWorkerID);
          lastRequestId.delete(pooledWorker.pooledWorkerID);
          workerIDMapping.delete(awsWorkerID);
          reverseMapping.delete(pooledWorker.pooledWorkerID);
          return;
        }
      } else {
        logInvokeTrace("WORKER_REUSE", requestID, `pooled=${pooledWorker.pooledWorkerID.slice(0, 8)}`);
        bus.publish("worker.reused", {
          workerID: awsWorkerID,
          functionID,
          pooledWorkerID: pooledWorker.pooledWorkerID,
        });
      }

      // Route invocation to the pooled worker
      const server = await getServer();
      logWorkers(`path=${requestPath} reqId=${requestID.slice(0, 8)} Routing invocation to worker ${pooledWorker.pooledWorkerID.slice(0, 8)} elapsed=${Date.now() - startTime}ms`);
      logInvokeTrace("ROUTE_INVOCATION", requestID);
      server.routeInvocation(pooledWorker.pooledWorkerID, evt.properties);
    } else {
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

        workers.set(awsWorkerID, {workerID: awsWorkerID, functionID});
        bus.publish("worker.started", {workerID: awsWorkerID, functionID});

        // Route invocation to the non-pooled worker
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
    }
  });

  // Process exit cleanup
  process.on("exit", () => {
    // Log final metrics summary
    writeSessionEndSummary();

    for (const pool of workerPool.values()) {
      for (const worker of pool) {
        clearTimeout(worker.idleTimer);
      }
    }
  });

  return {
    fromID(workerID: string) {
      // Check pooled workers first
      const pooled = activeWorkers.get(workerID);
      if (pooled) return {workerID, functionID: pooled.functionID};

      // Check non-pooled workers
      return workers.get(workerID)!;
    },

    getCurrentRequestID(workerID: string) {
      return lastRequestId.get(workerID);
    },

    stdout(workerID: string, message: string) {
      // Check pooled workers first
      const pooled = activeWorkers.get(workerID);
      if (pooled) {
        const requestID = lastRequestId.get(workerID);
        if (requestID) {
          bus.publish("worker.stdout", {
            workerID,
            functionID: pooled.functionID,
            message: message.trim(),
            requestID,
          });
        }
        return;
      }

      // Check if this is a preWarm worker (started but not yet active)
      if (startedWorkers.has(workerID)) {
        // During preWarm, ignore output since there's no request context
        return;
      }

      // Non-pooled worker
      const worker = workers.get(workerID);
      if (!worker) return;

      bus.publish("worker.stdout", {
        ...worker,
        message: message.trim(),
        requestID: lastRequestId.get(workerID)!,
      });
    },

    exited(workerID: string) {
      // Check if pooled worker
      if (activeWorkers.has(workerID) || startedWorkers.has(workerID)) {
        const worker = activeWorkers.get(workerID);
        if (worker) {
          const uptime = Date.now() - worker.createdAt;
          logPool("EXIT", {
            pooledWorkerID: workerID.slice(0, 8),
            functionID: worker.functionID,
            state: worker.state,
            uptimeMs: uptime,
          });

          // Clean up all mappings
          const awsWorkerID = reverseMapping.get(workerID);
          if (awsWorkerID) {
            workerIDMapping.delete(awsWorkerID);
          }
          reverseMapping.delete(workerID);
          activeWorkers.delete(workerID);
          lastRequestId.delete(workerID);
          startedWorkers.delete(workerID);

          bus.publish("worker.exited", {
            workerID: awsWorkerID || workerID,
            functionID: worker.functionID,
          });
        }
        return;
      }

      // Non-pooled worker
      const existing = workers.get(workerID);
      if (!existing) return;
      workers.delete(workerID);
      lastRequestId.delete(workerID);
      bus.publish("worker.exited", existing);
    },

    // Called by server when response is received - returns worker to pool
    onResponse(pooledWorkerID: string) {
      if (activeWorkers.has(pooledWorkerID)) {
        const worker = activeWorkers.get(pooledWorkerID);
        if (worker) {
          trackRequestEnd(worker.functionID);
          logPool("RESPONSE", {
            pooledWorkerID: pooledWorkerID.slice(0, 8),
            functionID: worker.functionID,
            requestID: lastRequestId.get(pooledWorkerID)?.slice(0, 8),
          });
        }
        returnToPool(pooledWorkerID);
      }
    },

    // Get AWS workerID from pooled ID (for IoT routing)
    getAwsWorkerID(pooledWorkerID: string): string | undefined {
      return reverseMapping.get(pooledWorkerID);
    },

    // Check if worker is pooled
    isPooled(workerID: string): boolean {
      return activeWorkers.has(workerID) || startedWorkers.has(workerID);
    },

    subscribe: bus.forward(
      "worker.started",
      "worker.stopped",
      "worker.exited",
      "worker.stdout",
      "worker.reused"
    ),

    /**
     * Trigger warmup by invoking Lambda functions with warmup payloads.
     * This sends real requests through the IoT bridge, which naturally creates workers.
     * @param count Number of workers to warm up (default: 15)
     */
    async triggerWarmup(count: number = 15) {
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
        logPool("WARMUP_SKIP", {
          reason: "no nodejs function found",
        });
        return { warmed: 0 };
      }

      const { functionName } = targetFunction;

      logPool("WARMUP_START", {
        count,
        functionName,
      });

      // Publish warmup start event
      bus.publish("warmup.start", { count });

      const startTime = Date.now();
      let success = 0;
      let failed = 0;
      let completed = 0;

      // Use the shared AWS client
      const { useAWSClient } = await import("../credentials.js");
      const { LambdaClient, InvokeCommand } = await import("@aws-sdk/client-lambda");
      const lambda = useAWSClient(LambdaClient);

      // Helper to publish progress
      const publishProgress = () => {
        bus.publish("warmup.progress", {
          completed,
          total: count,
          success,
          failed,
        });
      };

      // Phase 1: Invoke first warmup to populate V8 compile cache
      try {
        const result = await lambda.send(
          new InvokeCommand({
            FunctionName: functionName,
            InvocationType: "RequestResponse",
            Payload: JSON.stringify({
              ding: true,
              concurrency: count,
              index: 0,
            }),
          })
        );
        if (result.StatusCode === 200) {
          success++;
        } else {
          failed++;
        }
      } catch (ex: any) {
        failed++;
      }
      completed++;
      publishProgress();

      // Phase 2: Invoke remaining warmups in parallel
      if (count > 1) {
        const results = await Promise.all(
          Array.from({ length: count - 1 }, (_, i) => i + 1).map(async (i) => {
            try {
              const result = await lambda.send(
                new InvokeCommand({
                  FunctionName: functionName,
                  InvocationType: "RequestResponse",
                  Payload: JSON.stringify({
                    ding: true,
                    concurrency: count,
                    index: i,
                  }),
                })
              );
              const ok = result.StatusCode === 200;
              if (ok) success++;
              else failed++;
              completed++;
              publishProgress();
              return ok;
            } catch {
              failed++;
              completed++;
              publishProgress();
              return false;
            }
          })
        );
      }

      const elapsed = Date.now() - startTime;
      logPool("WARMUP_DONE", {
        success,
        failed,
        elapsedMs: elapsed,
        avgMs: success > 0 ? Math.round(elapsed / success) : 0,
      });

      // Publish warmup complete event
      bus.publish("warmup.complete", {
        success,
        failed,
        elapsedMs: elapsed,
      });

      return { warmed: success, elapsed };
    },
  };
});

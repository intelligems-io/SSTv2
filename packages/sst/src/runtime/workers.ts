import crypto from "crypto";
import { useBus } from "../bus.js";
import { useFunctionBuilder, useRuntimeHandlers } from "./handlers.js";
import { useRuntimeServerConfig, useRuntimeServer } from "./server.js";
import { useFunctions } from "../constructs/Function.js";
import { lazy } from "../util/lazy.js";
import { Logger } from "../logger.js";

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
}

// Configuration
const POOL_SIZE = parseInt(process.env.SST_WORKER_POOL_SIZE || "5", 10);
const IDLE_TIMEOUT = parseInt(
  process.env.SST_WORKER_IDLE_TIMEOUT || "30000",
  10
);

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

  // Lazy getter for server to avoid circular initialization
  let _server: Awaited<ReturnType<typeof useRuntimeServer>> | null = null;
  async function getServer() {
    if (!_server) {
      _server = await useRuntimeServer();
    }
    return _server;
  }

  // Helper: Terminate a pooled worker
  async function terminatePooledWorker(pooledWorkerID: string) {
    const worker =
      activeWorkers.get(pooledWorkerID) ||
      [...workerPool.values()]
        .flat()
        .find((w) => w.pooledWorkerID === pooledWorkerID);
    if (!worker) return;

    const props = useFunctions().fromID(worker.functionID);
    if (!props) return;

    const handler = handlers.for(props.runtime!);
    await handler?.stopWorker(pooledWorkerID);

    // Clean up mappings
    activeWorkers.delete(pooledWorkerID);
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
  function getIdleWorker(functionID: string): PooledWorker | undefined {
    const pool = workerPool.get(functionID);
    if (!pool || pool.length === 0) return undefined;

    const worker = pool.pop();
    if (worker) {
      clearTimeout(worker.idleTimer);
      worker.state = "busy";
      Logger.debug(
        "Reusing pooled worker",
        worker.pooledWorkerID,
        "for",
        functionID
      );
    }
    return worker;
  }

  // Helper: Return worker to pool
  function returnToPool(pooledWorkerID: string) {
    const worker = activeWorkers.get(pooledWorkerID);
    if (!worker) return;

    // Clean up current request mappings
    const awsWorkerID = reverseMapping.get(pooledWorkerID);
    if (awsWorkerID) {
      workerIDMapping.delete(awsWorkerID);
      reverseMapping.delete(pooledWorkerID);
    }

    // Check pool size
    let pool = workerPool.get(worker.functionID);
    if (!pool) {
      pool = [];
      workerPool.set(worker.functionID, pool);
    }

    if (pool.length >= POOL_SIZE) {
      // Pool full, terminate
      terminatePooledWorker(pooledWorkerID);
      Logger.debug("Pool full, terminated worker", pooledWorkerID);
      return;
    }

    // Return to pool with idle timeout
    worker.state = "idle";
    worker.idleTimer = setTimeout(() => {
      const idx = pool!.indexOf(worker);
      if (idx >= 0) pool!.splice(idx, 1);
      terminatePooledWorker(pooledWorkerID);
      Logger.debug("Idle timeout, terminated worker", pooledWorkerID);
    }, IDLE_TIMEOUT);

    pool.push(worker);
    activeWorkers.delete(pooledWorkerID);
    Logger.debug(
      "Returned worker to pool",
      pooledWorkerID,
      "pool size:",
      pool.length
    );
  }

  // Build success handler - clear pool for rebuilt function
  handlers.subscribe("function.build.success", async (evt) => {
    const { functionID } = evt.properties;

    // Clear pool for this function
    const pool = workerPool.get(functionID) || [];
    for (const worker of pool) {
      clearTimeout(worker.idleTimer);
      await terminatePooledWorker(worker.pooledWorkerID);
    }
    workerPool.delete(functionID);

    // Terminate active pooled workers for this function
    for (const [pooledID, worker] of activeWorkers) {
      if (worker.functionID === functionID) {
        await terminatePooledWorker(pooledID);
      }
    }

    // Stop non-pooled workers (legacy behavior)
    for (const [_, worker] of workers) {
      if (worker.functionID === functionID) {
        const props = useFunctions().fromID(worker.functionID);
        if (!props) return;
        const handler = handlers.for(props.runtime!);
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
    } = evt.properties;

    // Send ack immediately
    bus.publish("function.ack", { functionID, workerID: awsWorkerID });

    const props = useFunctions().fromID(functionID);
    if (!props) return;

    const handler = handlers.for(props.runtime!);
    if (!handler) return;

    const build = await builder.artifact(functionID);
    if (!build) return;

    // Check if this runtime supports pooling
    const poolable = isPoolableRuntime(props.runtime!, env);

    if (poolable) {
      // === POOLED PATH ===
      let pooledWorker = getIdleWorker(functionID);
      let isReuse = false;

      if (pooledWorker) {
        isReuse = true;
      } else {
        // Create new pooled worker
        const pooledWorkerID = crypto.randomBytes(16).toString("hex");
        pooledWorker = {
          pooledWorkerID,
          functionID,
          state: "busy",
          createdAt: Date.now(),
        };
      }

      // Set up mappings
      workerIDMapping.set(awsWorkerID, pooledWorker.pooledWorkerID);
      reverseMapping.set(pooledWorker.pooledWorkerID, awsWorkerID);
      lastRequestId.set(pooledWorker.pooledWorkerID, requestID);
      activeWorkers.set(pooledWorker.pooledWorkerID, pooledWorker);

      if (!isReuse) {
        // Start new worker with pooledWorkerID
        await handler.startWorker({
          ...build,
          workerID: pooledWorker.pooledWorkerID,
          functionID,
          environment: env,
          url: `${serverConfig.url}/${pooledWorker.pooledWorkerID}/${serverConfig.API_VERSION}`,
          runtime: props.runtime!,
        });
        startedWorkers.add(pooledWorker.pooledWorkerID);

        bus.publish("worker.started", {
          workerID: awsWorkerID,
          functionID,
        });
      } else {
        bus.publish("worker.reused", {
          workerID: awsWorkerID,
          functionID,
          pooledWorkerID: pooledWorker.pooledWorkerID,
        });
      }

      // Route invocation to the pooled worker
      const server = await getServer();
      server.routeInvocation(pooledWorker.pooledWorkerID, evt.properties);
    } else {
      // === NON-POOLED PATH (existing behavior) ===
      lastRequestId.set(awsWorkerID, requestID);

      let worker = workers.get(awsWorkerID);
      if (worker) return;

      await handler.startWorker({
        ...build,
        workerID: awsWorkerID,
        functionID,
        environment: env,
        url: `${serverConfig.url}/${awsWorkerID}/${serverConfig.API_VERSION}`,
        runtime: props.runtime!,
      });

      workers.set(awsWorkerID, { workerID: awsWorkerID, functionID });
      bus.publish("worker.started", { workerID: awsWorkerID, functionID });

      // Route invocation to the non-pooled worker
      const server = await getServer();
      server.routeInvocation(awsWorkerID, evt.properties);
    }
  });

  // Process exit cleanup
  process.on("exit", () => {
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
      if (pooled) return { workerID, functionID: pooled.functionID };

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
  };
});

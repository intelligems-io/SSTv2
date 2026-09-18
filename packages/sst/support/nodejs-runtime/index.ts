import { workerData, parentPort } from "node:worker_threads";
import { AsyncLocalStorage } from "node:async_hooks";
import path from "path";
import fs from "fs";
import http from "http";
import url from "url";
import os from "os";
import { Context as LambdaContext } from "aws-lambda";
import { tagLine } from "../../src/runtime/stdout-attribution.js";

// Enable V8 compile cache for faster subsequent worker starts (Node.js 22+)
// First worker compiles and caches bytecode, subsequent workers load from cache
try {
  const mod = await import("node:module");
  if (typeof mod.enableCompileCache === "function") {
    const cacheDir = path.join(os.tmpdir(), "sst-compile-cache");
    mod.enableCompileCache(cacheDir);
  }
} catch {
  // Node.js < 22.8 or compile cache not available - continue without it
}

interface WorkerInput {
  url: string;
  workerID: string;
  functionID: string;
  out: string;
  handler: string;
  isMonoBuild?: boolean;
  concurrency?: number;
  debugMemory?: boolean;
}

const input = workerData as WorkerInput;
const concurrency = Math.max(1, input.concurrency ?? 1);

// ---------------------------------------------------------------------------
// Per-invocation context
//
// One worker may run several invocations at once. Each invocation carries its
// own environment (the deployed function's env vars, plus SST_FUNCTION_ID for
// mono-build dispatch) and its own request id. Both live in AsyncLocalStorage,
// and `process.env` is replaced with a proxy that resolves against the current
// invocation first, so handler code that reads `process.env.X` keeps working
// unchanged while two functions with different env run side by side.
// ---------------------------------------------------------------------------

interface InvocationStore {
  requestID: string;
  env: Record<string, string>;
}

const invocation = new AsyncLocalStorage<InvocationStore>();
const baseEnv: Record<string, string | undefined> = { ...process.env };

function currentEnv(): Record<string, string | undefined> {
  return invocation.getStore()?.env ?? baseEnv;
}

process.env = new Proxy(baseEnv, {
  get(_, key) {
    if (typeof key !== "string") return undefined;
    const store = invocation.getStore();
    if (store && key in store.env) return store.env[key];
    return baseEnv[key];
  },
  set(_, key, value) {
    if (typeof key !== "string") return false;
    currentEnv()[key] = value === undefined ? undefined : String(value);
    return true;
  },
  has(_, key) {
    if (typeof key !== "string") return false;
    const store = invocation.getStore();
    return (store !== undefined && key in store.env) || key in baseEnv;
  },
  deleteProperty(_, key) {
    if (typeof key !== "string") return false;
    delete currentEnv()[key];
    return true;
  },
  ownKeys() {
    const store = invocation.getStore();
    return [...new Set([...Object.keys(baseEnv), ...(store ? Object.keys(store.env) : [])])];
  },
  getOwnPropertyDescriptor(_, key) {
    if (typeof key !== "string") return undefined;
    const store = invocation.getStore();
    const value = store && key in store.env ? store.env[key] : baseEnv[key];
    if (value === undefined && !(store && key in store.env) && !(key in baseEnv)) return undefined;
    return { value, writable: true, enumerable: true, configurable: true };
  },
}) as NodeJS.ProcessEnv;

// Tag console output with the request it belongs to so the parent can show
// interleaved logs under the right invocation.
for (const method of ["log", "info", "warn", "error", "debug", "trace"] as const) {
  const original = console[method].bind(console);
  console[method] = (...args: any[]) => {
    const store = invocation.getStore();
    if (!store) return original(...args);
    const text = args
      .map((a) => (typeof a === "string" ? a : safeFormat(a)))
      .join(" ");
    const tagged = text
      .split("\n")
      .map((line) => tagLine(store.requestID, line))
      .join("\n");
    return original(tagged);
  };
}

function safeFormat(value: unknown): string {
  if (value instanceof Error) return value.stack ?? value.message;
  if (typeof value === "object" && value !== null) {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

// ---------------------------------------------------------------------------
// Memory reporting (SST_DEBUG_MEMORY=true)
// ---------------------------------------------------------------------------

let lastMemoryReport = 0;
function reportMemory(phase: "loaded" | "response", loadMs?: number) {
  if (!input.debugMemory || !parentPort) return;
  const now = Date.now();
  // After responses, report at most every few seconds
  if (phase === "response" && now - lastMemoryReport < 5000) return;
  lastMemoryReport = now;
  const usage = process.memoryUsage();
  parentPort.postMessage({
    type: "sst.memory",
    report: {
      rss: usage.rss,
      heapUsed: usage.heapUsed,
      heapTotal: usage.heapTotal,
      external: usage.external,
      phase,
      loadMs,
    },
  });
}

// ---------------------------------------------------------------------------
// Handler loading
// ---------------------------------------------------------------------------

// Check for mono-bundle mode: use the flag passed from parent process, fallback to file check
const monoBundlePath = path.join(input.out, "index.mjs");
const useMonoBundle = input.isMonoBuild ?? (input.handler === "index.handler" && fs.existsSync(monoBundlePath));

let file: string;
let handlerName: string;

if (useMonoBundle) {
  // Mono-bundle mode: load from single bundled file
  file = monoBundlePath;
  handlerName = "handler"; // handler-functions.ts exports 'handler'
} else {
  // Individual handler mode (legacy)
  const parsed = path.parse(input.handler);
  const foundFile = [".js", ".jsx", ".mjs", ".cjs"]
    .map((ext) => path.join(input.out, parsed.dir, parsed.name + ext))
    .find((f) => fs.existsSync(f));

  if (!foundFile) {
    throw new Error(`Could not find handler file for "${input.handler}"`);
  }
  file = foundFile;
  handlerName = parsed.ext.substring(1);
}

let fn: any;

function fetch(req: {
  path: string;
  method: string;
  headers: Record<string, string>;
  body?: any;
}) {
  return new Promise<{
    statusCode: number;
    headers: Record<string, any>;
    body: string;
  }>((resolve, reject) => {
    const request = http.request(
      input.url + req.path,
      {
        headers: req.headers,
        method: req.method,
      },
      (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          body += chunk.toString();
        });

        res.on("end", () => {
          resolve({
            statusCode: res.statusCode!,
            headers: res.headers,
            body,
          });
        });
      }
    );
    request.on("error", reject);
    if (req.body) request.write(req.body);
    request.end();
  });
}

const loadStart = Date.now();
try {
  const { href } = url.pathToFileURL(file);
  const mod = await import(href);
  fn = mod[handlerName];
  if (!fn) {
    throw new Error(
      useMonoBundle
        ? `Mono-bundle handler "${handlerName}" not found in "${file}". Found: ${Object.keys(mod).join(", ")}`
        : `Function "${handlerName}" not found in "${input.handler}". Found: ${Object.keys(mod).join(", ")}`
    );
  }
} catch (ex: any) {
  await fetch({
    path: `/runtime/init/error`,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      errorType: "Error",
      errorMessage: ex.message,
      trace: ex.stack?.split("\n"),
    }),
  });
  process.exit(1);
}
reportMemory("loaded", Date.now() - loadStart);

// ---------------------------------------------------------------------------
// Invocation loops
// ---------------------------------------------------------------------------

// Exit if nothing has been asked of this worker for a while. The parent
// normally retires idle workers first; this is the backstop.
let idleTimer: NodeJS.Timeout | undefined;
function armIdleExit() {
  if (idleTimer) clearTimeout(idleTimer);
  idleTimer = setTimeout(() => {
    process.exit(0);
  }, 1000 * 60 * 15);
}

let lastContext: LambdaContext | undefined;

async function postError(context: LambdaContext | undefined, ex: any) {
  if (!context) return;
  await fetch({
    path: `/runtime/invocation/${context.awsRequestId}/error`,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      errorType: ex?.name ?? "Error",
      errorMessage: ex?.message ?? String(ex),
      trace: ex?.stack?.split("\n"),
    }),
  });
}

process.on("unhandledRejection", (ex) => {
  // Best effort: attribute to the invocation that was running most recently
  void postError(lastContext, ex);
});

async function runLoop() {
  while (true) {
    armIdleExit();

    let context: LambdaContext;
    let event: any;
    let env: Record<string, string> = {};
    try {
      const result = await fetch({
        path: `/runtime/invocation/next`,
        method: "GET",
        headers: {},
      });

      // Parse wrapped response: { event, env }
      const parsed = JSON.parse(result.body);
      if (parsed.env && typeof parsed.env === "object") {
        env = { ...parsed.env };
      }
      // For mono-build shared pool: function ID per invocation for dynamic dispatch
      const sstFunctionId = result.headers["lambda-runtime-sst-function-id"];
      if (sstFunctionId) {
        env.SST_FUNCTION_ID = sstFunctionId;
      }

      context = {
        awsRequestId: result.headers["lambda-runtime-aws-request-id"],
        invokedFunctionArn: result.headers["lambda-runtime-invoked-function-arn"],
        getRemainingTimeInMillis: () =>
          Math.max(
            Number(result.headers["lambda-runtime-deadline-ms"]) - Date.now(),
            0
          ),
        // If identity is null, we want to mimick AWS behavior and return undefined
        identity:
          JSON.parse(result.headers["lambda-runtime-cognito-identity"]) ??
          undefined,
        // If clientContext is null, we want to mimick AWS behavior and return undefined
        clientContext:
          JSON.parse(result.headers["lambda-runtime-client-context"]) ??
          undefined,
        // Per-invocation function context from headers (essential for mono-build shared workers)
        functionName: result.headers["lambda-runtime-function-name"] || env.AWS_LAMBDA_FUNCTION_NAME!,
        functionVersion: result.headers["lambda-runtime-function-version"] || env.AWS_LAMBDA_FUNCTION_VERSION!,
        memoryLimitInMB: result.headers["lambda-runtime-function-memory-size"] || env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE!,
        logGroupName: result.headers["lambda-runtime-log-group-name"],
        logStreamName: result.headers["lambda-runtime-log-stream-name"],
        callbackWaitsForEmptyEventLoop: {
          set value(_value: boolean) {
            throw new Error(
              "`callbackWaitsForEmptyEventLoop` on lambda Context is not implemented by SST Live Lambda Development."
            );
          },
          get value() {
            return true;
          },
        }.value,
        done() {
          throw new Error(
            "`done` on lambda Context is not implemented by SST Live Lambda Development."
          );
        },
        fail() {
          throw new Error(
            "`fail` on lambda Context is not implemented by SST Live Lambda Development."
          );
        },
        succeed() {
          throw new Error(
            "`succeed` on lambda Context is not implemented by SST Live Lambda Development."
          );
        },
      };
      event = parsed.event;
    } catch {
      continue;
    }

    // Compatibility for code that snapshots process.env outside any
    // invocation (module scope): the base env follows the latest invocation.
    Object.assign(baseEnv, env);
    lastContext = context;
    (global as any)[Symbol.for("aws.lambda.runtime.requestId")] = context.awsRequestId;

    const store: InvocationStore = { requestID: context.awsRequestId, env };
    let response: any;
    try {
      response = await invocation.run(store, () => fn(event, context));
    } catch (ex: any) {
      await postError(context, ex);
      continue;
    }

    while (true) {
      try {
        await fetch({
          path: `/runtime/invocation/${context.awsRequestId}/response`,
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(response),
        });
        break;
      } catch (ex) {
        console.error(ex);
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }
    reportMemory("response");
  }
}

await Promise.all(Array.from({ length: concurrency }, () => runLoop()));

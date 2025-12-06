import { workerData } from "node:worker_threads";
import path from "path";
import fs from "fs";
import http from "http";
import url from "url";
import os from "os";
import { Context as LambdaContext } from "aws-lambda";
// import { createRequire } from "module";
// global.require = createRequire(import.meta.url);

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

const input = workerData;

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

  // For mono-bundle: eagerly load one handler to warm the shared chunks
  // This triggers loading of the 35MB+ shared chunks that are lazy-loaded
  if (useMonoBundle && mod.warmUp) {
    await mod.warmUp();
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

let timeout: NodeJS.Timeout | undefined;
let request: any;
let response: any;
let context: LambdaContext;

async function error(ex: any) {
  await fetch({
    path: `/runtime/invocation/${context.awsRequestId}/error`,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      errorType: ex.name ?? "Error",
      errorMessage: ex.message,
      trace: ex.stack?.split("\n"),
    }),
  });
}
process.on("unhandledRejection", error);
while (true) {
  if (timeout) clearTimeout(timeout);
  timeout = setTimeout(() => {
    process.exit(0);
  }, 1000 * 60 * 15);

  try {
    const result = await fetch({
      path: `/runtime/invocation/next`,
      method: "GET",
      headers: {},
    });

    // For mono-build shared pool: set function ID per-invocation for dynamic dispatch
    const sstFunctionId = result.headers["lambda-runtime-sst-function-id"];
    if (sstFunctionId) {
      process.env.SST_FUNCTION_ID = sstFunctionId;
    }

    // Parse wrapped response: { event, env }
    // Apply per-invocation env vars to prevent leakage when workers are reused
    const parsed = JSON.parse(result.body);
    const invocationEnv = parsed.env;
    if (invocationEnv && typeof invocationEnv === "object") {
      Object.assign(process.env, invocationEnv);
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
      functionName: result.headers["lambda-runtime-function-name"] || process.env.AWS_LAMBDA_FUNCTION_NAME!,
      functionVersion: result.headers["lambda-runtime-function-version"] || process.env.AWS_LAMBDA_FUNCTION_VERSION!,
      memoryLimitInMB: result.headers["lambda-runtime-function-memory-size"] || process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE!,
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
    request = parsed.event;
  } catch {
    continue;
  }
  (global as any)[Symbol.for("aws.lambda.runtime.requestId")] =
    context.awsRequestId;

  try {
    response = await fn(request, context);
  } catch (ex: any) {
    error(ex);
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
}

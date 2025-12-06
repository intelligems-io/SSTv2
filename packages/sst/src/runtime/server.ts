import express from "express";
import { Events, useBus } from "../bus.js";
import { Logger } from "../logger.js";
import { useRuntimeWorkers } from "./workers.js";
import { useFunctions } from "../constructs/Function.js";
import https from "https";
import getPort from "get-port";
import { lazy } from "../util/lazy.js";
import { getRequestPath } from "./request-utils.js";

export const useRuntimeServerConfig = lazy(async () => {
  const port = await getPort({
    port: 12557,
  });
  return {
    API_VERSION: "2018-06-01",
    port,
    url: `http://localhost:${port}`,
  };
});

export const useRuntimeServer = lazy(async () => {
  const bus = useBus();
  const app = express();
  const workers = await useRuntimeWorkers();
  const cfg = await useRuntimeServerConfig();

  const workersWaiting = new Map<
    string,
    (evt: Events["function.invoked"]) => void
  >();
  const invocationsQueued = new Map<string, Events["function.invoked"][]>();

  function next(workerID: string) {
    const queue = invocationsQueued.get(workerID);
    const value = queue?.shift();
    if (value) return value;

    return new Promise<Events["function.invoked"]>((resolve, reject) => {
      workersWaiting.set(workerID, resolve);
    });
  }

  // Route an invocation to a specific workerID (used by workers.ts for pooled workers)
  function routeInvocation(
    targetWorkerID: string,
    invocation: Events["function.invoked"]
  ) {
    const requestPath = getRequestPath(invocation.event);
    const requestID = invocation.requestID;

    const waiting = workersWaiting.get(targetWorkerID);
    if (waiting) {
      workersWaiting.delete(targetWorkerID);
      console.log(`[SERVER] path=${requestPath} reqId=${requestID.slice(0, 8)} Worker ${targetWorkerID.slice(0, 8)} was waiting, delivering immediately`);
      waiting(invocation);
      return;
    }

    let arr = invocationsQueued.get(targetWorkerID);
    if (!arr) {
      arr = [];
      invocationsQueued.set(targetWorkerID, arr);
    }
    arr.push(invocation);
    console.log(`[SERVER] path=${requestPath} reqId=${requestID.slice(0, 8)} Worker ${targetWorkerID.slice(0, 8)} not waiting, QUEUED (queueSize=${arr.length})`);
  }

  workers.subscribe("worker.exited", async (evt) => {
    const waiting = workersWaiting.get(evt.properties.workerID);
    if (!waiting) return;
    workersWaiting.delete(evt.properties.workerID);
  });

  // Note: function.invoked routing is handled by workers.ts via routeInvocation()
  // This ensures correct routing for both pooled and non-pooled workers

  app.post<{ functionID: string; workerID: string }>(
    `/:workerID/${cfg.API_VERSION}/runtime/init/error`,
    express.json({
      strict: false,
      type: ["application/json", "application/*+json"],
      limit: "10mb",
    }),
    async (req, res) => {
      const pooledWorkerID = req.params.workerID;
      const worker = workers.fromID(pooledWorkerID);
      if (!worker) {
        res.status(404).send();
        return;
      }

      // Get AWS workerID for IoT routing (if pooled)
      const awsWorkerID = workers.isPooled(pooledWorkerID)
        ? workers.getAwsWorkerID(pooledWorkerID) || pooledWorkerID
        : pooledWorkerID;

      bus.publish("function.error", {
        requestID: workers.getCurrentRequestID(pooledWorkerID),
        workerID: awsWorkerID,
        functionID: worker.functionID,
        ...req.body,
      });

      // Return pooled worker to pool
      workers.onResponse(pooledWorkerID);

      res.json("ok");
    }
  );

  app.get<{ functionID: string; workerID: string }>(
    `/:workerID/${cfg.API_VERSION}/runtime/invocation/next`,
    async (req, res) => {
      const workerID = req.params.workerID;
      console.log(`[SERVER] Worker ${workerID.slice(0, 8)} requesting /next`);
      Logger.debug(
        "Worker",
        workerID,
        "is waiting for next invocation"
      );
      const waitStart = Date.now();
      const payload = await next(workerID);
      const requestPath = getRequestPath(payload.event);
      const requestID = payload.context.awsRequestId;
      console.log(`[SERVER] path=${requestPath} reqId=${requestID.slice(0, 8)} Worker ${workerID.slice(0, 8)} received payload after waiting ${Date.now() - waitStart}ms`);
      Logger.debug("Worker", workerID, "sending next payload");

      // Get function properties for per-invocation context
      // This is essential for mono-build shared workers that serve multiple functions
      const funcProps = useFunctions().fromID(payload.functionID);

      res.set({
        "Lambda-Runtime-Aws-Request-Id": requestID,
        "Lambda-Runtime-Deadline-Ms": Date.now() + payload.deadline,
        "Lambda-Runtime-Invoked-Function-Arn":
          payload.context.invokedFunctionArn,
        "Lambda-Runtime-Client-Context": JSON.stringify(
          payload.context.clientContext || null
        ),
        "Lambda-Runtime-Cognito-Identity": JSON.stringify(
          payload.context.identity || null
        ),
        "Lambda-Runtime-Log-Group-Name": payload.context.logGroupName,
        "Lambda-Runtime-Log-Stream-Name": payload.context.logStreamName,
        // Pass function ID for mono-build shared pool: allows per-invocation dispatch
        "Lambda-Runtime-Sst-Function-Id": payload.functionID,
      });
      // Wrap event with env for per-invocation environment variable application
      // This prevents env leakage when workers are reused across different functions
      res.json({
        event: payload.event,
        env: payload.env,
      });
    }
  );

  app.post<{
    workerID: string;
    awsRequestId: string;
  }>(
    `/:workerID/${cfg.API_VERSION}/runtime/invocation/:awsRequestId/response`,
    express.json({
      strict: false,
      type() {
        return true;
      },
      limit: "10mb",
    }),
    (req, res) => {
      const pooledWorkerID = req.params.workerID;
      const requestID = req.params.awsRequestId;
      console.log(`[SERVER] reqId=${requestID.slice(0, 8)} Worker ${pooledWorkerID.slice(0, 8)} posting /response`);
      Logger.debug("Worker", pooledWorkerID, "got response", req.body);

      const worker = workers.fromID(pooledWorkerID);
      if (!worker) {
        console.log(`[SERVER] reqId=${requestID.slice(0, 8)} ERROR: Worker ${pooledWorkerID.slice(0, 8)} not found`);
        res.status(404).send();
        return;
      }

      // Get AWS workerID for IoT routing (if pooled)
      const awsWorkerID = workers.isPooled(pooledWorkerID)
        ? workers.getAwsWorkerID(pooledWorkerID) || pooledWorkerID
        : pooledWorkerID;

      console.log(`[SERVER] reqId=${requestID.slice(0, 8)} Publishing function.success awsWorkerID=${awsWorkerID.slice(0, 8)}`);
      bus.publish("function.success", {
        workerID: awsWorkerID,
        functionID: worker.functionID,
        requestID: requestID,
        body: req.body,
      });

      // Return pooled worker to pool
      workers.onResponse(pooledWorkerID);

      res.status(202).send();
    }
  );

  app.all<{
    href: string;
  }>(
    `/proxy*`,
    express.raw({
      type: "*/*",
      limit: "1024mb",
    }),
    (req, res) => {
      res.header("Access-Control-Allow-Origin", "*");
      res.header(
        "Access-Control-Allow-Methods",
        "GET, PUT, PATCH, POST, DELETE"
      );
      res.header(
        "Access-Control-Allow-Headers",
        req.header("access-control-request-headers")
      );

      if (req.method === "OPTIONS") return res.send();
      const u = new URL(req.url.substring(7));
      const forward = https.request(
        u,
        {
          headers: {
            ...req.headers,
            host: u.hostname,
          },
          method: req.method,
        },
        (proxied) => {
          res.status(proxied.statusCode!);
          for (const [key, value] of Object.entries(proxied.headers)) {
            res.header(key, value);
          }
          proxied.pipe(res);
        }
      );
      if (
        req.method !== "GET" &&
        req.method !== "DELETE" &&
        req.method !== "HEAD" &&
        req.body
      )
        forward.write(req.body);
      forward.end();
      forward.on("error", (e) => {
        console.log(e.message);
      });
    }
  );

  app.post<{
    workerID: string;
    awsRequestId: string;
  }>(
    `/:workerID/${cfg.API_VERSION}/runtime/invocation/:awsRequestId/error`,
    express.json({
      strict: false,
      type: ["application/json", "application/*+json"],
      limit: "10mb",
    }),
    (req, res) => {
      const pooledWorkerID = req.params.workerID;
      const worker = workers.fromID(pooledWorkerID);
      if (!worker) {
        res.status(404).send();
        return;
      }

      // Get AWS workerID for IoT routing (if pooled)
      const awsWorkerID = workers.isPooled(pooledWorkerID)
        ? workers.getAwsWorkerID(pooledWorkerID) || pooledWorkerID
        : pooledWorkerID;

      bus.publish("function.error", {
        workerID: awsWorkerID,
        functionID: worker.functionID,
        errorType: req.body.errorType,
        errorMessage: req.body.errorMessage,
        requestID: req.params.awsRequestId,
        trace: req.body.trace,
      });

      // Return pooled worker to pool
      workers.onResponse(pooledWorkerID);

      res.status(202).send();
    }
  );

  app.listen(cfg.port);

  return {
    routeInvocation,
  };
});

import { useBus } from "../bus.js";
import { useIOT, useIOTControl } from "../iot.js";
import { lazy } from "../util/lazy.js";
import { logInvokeTrace } from "./worker-pool-logging.js";
import { logIot } from "./debug-bridge-logging.js";
import { logEventTrace } from "./event-trace-logging.js";

export const useIOTBridge = lazy(async () => {
  const bus = useBus();
  const iot = await useIOT();
  // The ack goes out on its own socket so it can never queue behind a response
  // body. See `useIOTControl`.
  const control = await useIOTControl();
  const topic = `${iot.prefix}/events`;

  bus.subscribe("function.success", async (evt) => {
    const { workerID, requestID } = evt.properties;
    logIot(`reqId=${requestID?.slice(0, 8)} Publishing function.success to worker ${workerID.slice(0, 8)}`);
    const startTime = Date.now();
    await iot.publish(
      topic + "/" + workerID,
      "function.success",
      evt.properties
    );
    logIot(`reqId=${requestID?.slice(0, 8)} function.success published in ${Date.now() - startTime}ms`);
  });
  bus.subscribe("function.error", async (evt) => {
    const { workerID, requestID } = evt.properties;
    logIot(`reqId=${requestID?.slice(0, 8)} Publishing function.error to worker ${workerID.slice(0, 8)}`);
    const startTime = Date.now();
    await iot.publish(
      topic + "/" + workerID,
      "function.error",
      evt.properties
    );
    logIot(`reqId=${requestID?.slice(0, 8)} function.error published in ${Date.now() - startTime}ms`);
  });
  bus.subscribe("function.ack", async (evt) => {
    const { workerID, requestID, functionID } = evt.properties;
    logIot(`reqId=${requestID?.slice(0, 8)} Publishing function.ack to worker ${workerID.slice(0, 8)}`);
    logInvokeTrace("IOT_ACK_START", workerID, `worker=${workerID.slice(0, 8)}`);
    const startTime = Date.now();
    await control.publish(
      topic + "/" + workerID,
      "function.ack",
      evt.properties
    );
    const elapsed = Date.now() - startTime;
    logIot(`reqId=${requestID?.slice(0, 8)} function.ack published in ${elapsed}ms`);
    logInvokeTrace("IOT_ACK_DONE", workerID, `worker=${workerID.slice(0, 8)}`);
    logEventTrace("IOT_ACK", {
      requestID,
      functionID,
      workerID,
      elapsed,
    });
  });
});

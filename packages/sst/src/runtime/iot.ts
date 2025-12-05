import { useBus } from "../bus.js";
import { useIOT } from "../iot.js";
import { lazy } from "../util/lazy.js";
import { logInvokeTrace } from "./worker-pool-logging.js";

export const useIOTBridge = lazy(async () => {
  const bus = useBus();
  const iot = await useIOT();
  const topic = `${iot.prefix}/events`;

  bus.subscribe("function.success", async (evt) => {
    iot.publish(
      topic + "/" + evt.properties.workerID,
      "function.success",
      evt.properties
    );
  });
  bus.subscribe("function.error", async (evt) => {
    iot.publish(
      topic + "/" + evt.properties.workerID,
      "function.error",
      evt.properties
    );
  });
  bus.subscribe("function.ack", async (evt) => {
    const workerID = evt.properties.workerID;
    logInvokeTrace("IOT_ACK_START", workerID, `worker=${workerID.slice(0, 8)}`);
    await iot.publish(
      topic + "/" + workerID,
      "function.ack",
      evt.properties
    );
    logInvokeTrace("IOT_ACK_DONE", workerID, `worker=${workerID.slice(0, 8)}`);
  });
});

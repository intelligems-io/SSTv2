import { IoTClient, DescribeEndpointCommand } from "@aws-sdk/client-iot";
import { useAWSClient, useAWSCredentials } from "./credentials.js";
import { VisibleError } from "./error.js";
import { lazy } from "./util/lazy.js";
import { Logger } from "./logger.js";
import { logIotRx } from "./runtime/debug-bridge-logging.js";
import { logEventTrace } from "./runtime/event-trace-logging.js";
import { getRequestPath, getCorrelationId, getApiGatewayRequestId, getHttpMethod } from "./runtime/request-utils.js";

export const useIOTEndpoint = lazy(async () => {
  const iot = useAWSClient(IoTClient);
  Logger.debug("Getting IoT endpoint");
  const response = await iot.send(
    new DescribeEndpointCommand({
      endpointType: "iot:Data-ATS",
    })
  );
  Logger.debug("Using IoT endpoint:", response.endpointAddress);

  if (!response.endpointAddress)
    throw new VisibleError("IoT Endpoint address not found");

  return response.endpointAddress;
});

import iot from "aws-iot-device-sdk";
import { EventPayload, Events, EventTypes, useBus } from "./bus.js";
import { useProject } from "./project.js";
import { useBootstrap } from "./bootstrap.js";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

interface Fragment {
  id: string;
  index: number;
  count: number;
  data: string;
}

export const useIOT = lazy(async () => {
  const bus = useBus();

  const endpoint = await useIOTEndpoint();
  const creds = await useAWSCredentials();
  const project = useProject();
  const bootstrap = await useBootstrap();
  const s3 = useAWSClient(S3Client);

  async function encode(input: any) {
    const id = Math.random().toString();
    const json = JSON.stringify(input);
    if (json.length > 1024 * 1024 * 3) {
      // upload to s3
      const key = `pointers/${id}`;
      await s3.send(
        new PutObjectCommand({
          Bucket: bootstrap.bucket,
          Key: key,
          Body: json,
        })
      );

      return [
        {
          id,
          index: 0,
          count: 1,
          data: JSON.stringify({
            type: "pointer",
            properties: {
              key,
              bucket: bootstrap.bucket,
            },
          }),
        },
      ];
    }
    const parts = json.match(/.{1,50000}/g);
    if (!parts) return [];
    Logger.debug("Encoded iot message into", parts?.length, "parts");
    return parts.map((part, index) => ({
      id,
      index,
      count: parts?.length,
      data: part,
    }));
  }
  /*
  console.log(endpoint, creds);
  const config =
    iotsdk.iot.AwsIotMqttConnectionConfigBuilder.new_with_websockets({
      region: project.config.region!,
      credentials_provider: iotsdk.auth.AwsCredentialsProvider.newStatic(
        creds.accessKeyId,
        creds.secretAccessKey,
        creds.sessionToken
      ),
    })
      .with_client_id(randomUUID())
      .with_endpoint(endpoint)
      .build();
  console.log(config);

  const device2 = new iotsdk.mqtt.MqttClient();
  const conn = device2.new_connection(config);
  conn.on("connect", () => {
    console.log("CONNECTED");
  });
  conn.on("error", console.log);
  conn.on("connect", console.log);
  conn.on("resume", console.log);
  conn.on("message", console.log);
  conn.on("disconnect", console.log);
  conn.connect();
  console.log(device2);
  */

  const device = new iot.device({
    protocol: "wss",
    host: endpoint,
    region: project.config.region,
    accessKeyId: creds.accessKeyId,
    secretKey: creds.secretAccessKey,
    sessionToken: creds.sessionToken,
    reconnectPeriod: 1,
    keepalive: 60,
  });
  const PREFIX = `/sst/${project.config.name}/${project.config.stage}`;
  device.subscribe(`${PREFIX}/events`, { qos: 1 });

  const fragments = new Map<string, Map<number, Fragment>>();

  device.on("connect", () => {
    Logger.debug("IoT connected");
  });

  device.on("error", (err) => {
    Logger.debug("IoT error", err);
  });

  device.on("close", () => {
    Logger.debug("IoT closed");
  });

  device.on("reconnect", () => {
    Logger.debug("IoT reconnecting...");
  });

  device.on("message", (_topic, buffer: Buffer) => {
    const fragment = JSON.parse(buffer.toString());
    if (!fragment.id) {
      const requestID = fragment.properties?.requestID;
      logIotRx(`Received ${fragment.type} reqId=${requestID?.slice(0, 8) || 'N/A'}`);
      if (fragment.type === "function.invoked" && requestID) {
        const event = fragment.properties?.event;
        logEventTrace("IOT_RECEIVED", {
          requestID,
          functionID: fragment.properties?.functionID,
          path: getRequestPath(event),
          method: getHttpMethod(event),
          correlationId: getCorrelationId(event),
          apiGwReqId: getApiGatewayRequestId(event),
        });
      }
      bus.publish(fragment.type, fragment.properties);
      return;
    }

    let pending = fragments.get(fragment.id);
    if (!pending) {
      pending = new Map();
      fragments.set(fragment.id, pending);
    }
    pending.set(fragment.index, fragment);

    if (pending.size === fragment.count) {
      const data = [...pending.values()]
        .sort((a, b) => a.index - b.index)
        .map((item) => item.data)
        .join("");
      fragments.delete(fragment.id);
      const evt = JSON.parse(data) as EventPayload;
      if (evt.sourceID === bus.sourceID) return;
      const requestID = (evt.properties as any)?.requestID;
      logIotRx(`Received ${evt.type} reqId=${requestID?.slice(0, 8) || 'N/A'} (${fragment.count} fragments)`);
      if (evt.type === "function.invoked" && requestID) {
        const event = (evt.properties as any)?.event;
        logEventTrace("IOT_RECEIVED", {
          requestID,
          functionID: (evt.properties as any)?.functionID,
          path: getRequestPath(event),
          method: getHttpMethod(event),
          correlationId: getCorrelationId(event),
          apiGwReqId: getApiGatewayRequestId(event),
          fragments: fragment.count,
        });
      }
      bus.publish(evt.type, evt.properties);
    }
  });

  return {
    prefix: PREFIX,
    async publish<Type extends EventTypes>(
      topic: string,
      type: Type,
      properties: Events[Type]
    ) {
      const payload: EventPayload = {
        type,
        properties,
        sourceID: bus.sourceID,
      };
      const fragments = await encode(payload);
      await Promise.all(
        fragments.map(
          (fragment) =>
            new Promise<void>((r) => {
              device.publish(topic, JSON.stringify(fragment), { qos: 1 }, () => {
              r();
        });
            })
        )
      );
      Logger.debug("IOT Published", topic, type);
    },
  };
});

/**
 * A second connection, carrying only small control messages.
 *
 * `useIOT` also carries response bodies. Those are fragmented into 50KB chunks
 * and published at QoS 1, so every chunk waits on a broker round trip, and they
 * queue on the same socket as everything else.
 *
 * `function.ack` cannot afford to wait behind them. The Lambda re-invokes if the
 * ack does not arrive within two seconds, and each re-invocation re-runs the
 * handler and puts another response body on the socket that is already busy —
 * so a late ack makes the next ack later still. Measured on one machine before
 * this change: ack publish went from 105ms to 10.2s inside a minute, publishes
 * in flight from 15 to 439, and a single request executed fifteen times before
 * the gateway returned 504 at 29s.
 *
 * Round-trip latency decides who sees it. A chunk costs a round trip to the
 * region — roughly 90ms from Europe against 15ms from the US — so the same
 * payload lands either side of the two second budget depending on where the
 * developer sits.
 *
 * **The client id must differ from `useIOT`'s.** AWS IoT drops an existing
 * connection when a new one presents the same id, so a shared id would make the
 * two sockets disconnect each other in a loop.
 *
 * This connection deliberately does not subscribe. Inbound traffic still arrives
 * on `useIOT`; this one only publishes.
 */
export const useIOTControl = lazy(async () => {
  const bus = useBus();
  const endpoint = await useIOTEndpoint();
  const creds = await useAWSCredentials();
  const project = useProject();

  const device = new iot.device({
    protocol: "wss",
    host: endpoint,
    region: project.config.region,
    accessKeyId: creds.accessKeyId,
    secretKey: creds.secretAccessKey,
    sessionToken: creds.sessionToken,
    reconnectPeriod: 1,
    keepalive: 60,
    clientId: `sst-control-${Math.random().toString(16).slice(2)}`,
  });
  const PREFIX = `/sst/${project.config.name}/${project.config.stage}`;

  device.on("connect", () => {
    Logger.debug("IoT control connected");
  });
  device.on("error", (err) => {
    Logger.debug("IoT control error", err);
  });
  device.on("close", () => {
    Logger.debug("IoT control closed");
  });
  device.on("reconnect", () => {
    Logger.debug("IoT control reconnecting...");
  });

  return {
    prefix: PREFIX,
    async publish<Type extends EventTypes>(
      topic: string,
      type: Type,
      properties: Events[Type]
    ) {
      const payload: EventPayload = {
        type,
        properties,
        sourceID: bus.sourceID,
      };
      // Control messages sit far below the fragment size, but the envelope has
      // to match what the Lambda reassembles, so it is built the same way
      // rather than assumed to be a single part.
      const json = JSON.stringify(payload);
      const parts = json.match(/.{1,50000}/g) ?? [];
      const id = Math.random().toString();
      await Promise.all(
        parts.map(
          (data, index) =>
            new Promise<void>((r) => {
              device.publish(
                topic,
                JSON.stringify({ id, index, count: parts.length, data }),
                { qos: 1 },
                () => {
                  r();
                }
              );
            })
        )
      );
      Logger.debug("IOT Control Published", topic, type);
    },
  };
});

export const isSupported = () =>
  [
    "eu-central-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-north-1",
    "ap-south-1",
    "ap-northeast-2",
    "ap-northeast-1",
    "me-south-1",
    "me-central-1",
    "sa-east-1",
    "ca-central-1",
    "ap-east-1",
    "ap-southeast-1",
    "ap-southeast-2",
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "us-gov-east-1",
    "us-gov-west-1",
    "cn-north-1",
    "cn-northwest-1",
  ].includes(useProject().config.region!);

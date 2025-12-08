import { createDebugFileLogger, DebugFileLogger } from "./debug-file-logger.js";

/**
 * Event Trace Logging for SST Dev Bridge
 *
 * Tracks the complete lifecycle of a request from IOT reception to worker completion.
 * All events share a common `requestID` for end-to-end tracing.
 *
 * Enable with: SST_EVENT_TRACE=true
 *
 * Log file: .sst/event-trace.log
 *
 * Event Types:
 * - IOT_RECEIVED     : IOT event received from Lambda
 * - IOT_ACK          : IOT acknowledgment published
 * - WORKER_START     : Worker function started
 * - WORKER_LOG       : Worker function log with [LOG] prefix
 * - WORKER_END       : Worker function completed (success/error)
 *
 * Tracing from Frontend:
 * - Send X-Correlation-ID header from frontend for easy correlation
 * - Or use the path to filter by endpoint
 * - Or use apiGwReqId (visible in browser as x-amzn-requestid response header)
 */

const EVENT_TRACE_ENABLED = process.env.SST_EVENT_TRACE === "true";

let eventLogger: DebugFileLogger | null = null;

function getEventLogger(): DebugFileLogger | null {
  if (!EVENT_TRACE_ENABLED) return null;
  if (!eventLogger) {
    eventLogger = createDebugFileLogger({
      filePath: ".sst/event-trace.log",
      sessionName: "EVENT_TRACE",
      width: 140,
    });
  }
  return eventLogger;
}

type EventType =
  | "IOT_RECEIVED"
  | "IOT_ACK"
  | "WORKER_START"
  | "WORKER_LOG"
  | "WORKER_END";

interface EventTraceDetails {
  requestID: string;
  functionID?: string;
  workerID?: string;
  message?: string;
  status?: "success" | "error";
  elapsed?: number;
  [key: string]: any;
}

/**
 * Log an event trace entry with requestID as the common identifier
 *
 * @param event - The event type
 * @param details - Event details including requestID (required)
 *
 * @example
 * logEventTrace("IOT_RECEIVED", { requestID: "abc123", functionID: "MyFunc" });
 * logEventTrace("WORKER_LOG", { requestID: "abc123", message: "[LOG] User action" });
 * logEventTrace("WORKER_END", { requestID: "abc123", status: "success", elapsed: 150 });
 */
export function logEventTrace(event: EventType, details: EventTraceDetails) {
  const logger = getEventLogger();
  if (!logger) return;

  // Format requestID for consistent display (first 8 chars)
  const reqId = details.requestID?.slice(0, 8) || "N/A";

  // Build the log message with requestID prominently displayed
  const logDetails: Record<string, any> = {
    reqId,
    ...details,
  };

  // Remove the full requestID since we're using the shortened version
  delete logDetails.requestID;

  logger.log(event, logDetails);
}

/**
 * Check if event trace logging is enabled
 */
export function isEventTraceEnabled(): boolean {
  return EVENT_TRACE_ENABLED;
}

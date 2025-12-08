import {createDebugFileLogger, DebugFileLogger} from "./debug-file-logger.js";

/**
 * Debug logging for SST dev bridge components
 *
 * Enable with: SST_DEBUG_BRIDGE=true
 *
 * Log files:
 * - .sst/debug-workers.log - Worker pool and invocation handling
 * - .sst/debug-server.log  - Runtime server request/response handling
 * - .sst/debug-iot.log     - IoT message publishing and receiving
 */

const DEBUG_BRIDGE = process.env.SST_DEBUG_BRIDGE === "true";

// Lazy-initialized loggers
let workersLogger: DebugFileLogger | null = null;
let serverLogger: DebugFileLogger | null = null;
let iotLogger: DebugFileLogger | null = null;

function getWorkersLogger(): DebugFileLogger | null {
  if (!DEBUG_BRIDGE) return null;
  if (!workersLogger) {
    workersLogger = createDebugFileLogger({
      filePath: ".sst/debug-workers.log",
      sessionName: "WORKERS",
      width: 120,
    });
  }
  return workersLogger;
}

function getServerLogger(): DebugFileLogger | null {
  if (!DEBUG_BRIDGE) return null;
  if (!serverLogger) {
    serverLogger = createDebugFileLogger({
      filePath: ".sst/debug-server.log",
      sessionName: "SERVER",
      width: 120,
    });
  }
  return serverLogger;
}

function getIotLogger(): DebugFileLogger | null {
  if (!DEBUG_BRIDGE) return null;
  if (!iotLogger) {
    iotLogger = createDebugFileLogger({
      filePath: ".sst/debug-iot.log",
      sessionName: "IOT",
      width: 120,
    });
  }
  return iotLogger;
}

/**
 * Log debug message for workers component
 * Outputs to console and optionally to .sst/debug-workers.log
 */
export function logWorkers(message: string, details?: Record<string, any>) {
  const logger = getWorkersLogger();
  if (logger) {
    logger.log("WORKERS", { msg: message, ...details });
  }
}

/**
 * Log debug message for server component
 * Outputs to console and optionally to .sst/debug-server.log
 */
export function logServer(message: string, details?: Record<string, any>) {
  const logger = getServerLogger();
  if (logger) {
    logger.log("SERVER", { msg: message, ...details });
  }
}

/**
 * Log debug message for IoT component (publishing)
 * Outputs to console and optionally to .sst/debug-iot.log
 */
export function logIot(message: string, details?: Record<string, any>) {
  const logger = getIotLogger();
  if (logger) {
    logger.log("IOT", { msg: message, ...details });
  }
}

/**
 * Log debug message for IoT component (receiving)
 * Outputs to console and optionally to .sst/debug-iot.log
 */
export function logIotRx(message: string, details?: Record<string, any>) {
  const logger = getIotLogger();
  if (logger) {
    logger.log("IOT-RX", { msg: message, ...details });
  }
}

/**
 * Check if bridge debug logging is enabled
 */
export function isDebugBridgeEnabled(): boolean {
  return DEBUG_BRIDGE;
}

import fs from "fs";
import path from "path";

/**
 * Configuration for creating a debug file logger
 */
export interface DebugFileLoggerConfig {
  /** Log file path (relative paths are relative to CWD) */
  filePath: string;
  /** Optional environment variable to enable/disable logging (if not set, logging is always enabled) */
  envVar?: string;
  /** Name for session markers (e.g., "POOL", "TRACE") */
  sessionName: string;
  /** Optional header line content for session start */
  sessionHeader?: string;
  /** Column width for padding (default: 80) */
  width?: number;
}

/**
 * Debug file logger instance
 */
export interface DebugFileLogger {
  /** Log a message with timestamp and formatted details */
  log: (action: string, details?: Record<string, any>) => void;
  /** Log a raw line (no formatting) */
  logRaw: (line: string) => void;
  /** Check if logging is enabled */
  isEnabled: () => boolean;
  /** Write session end marker and close the stream */
  close: (summary?: string) => void;
  /** Get the write stream (for advanced use cases) */
  getStream: () => fs.WriteStream | null;
}

// Active loggers registry (for cleanup)
const activeLoggers = new Map<string, fs.WriteStream>();

// Cleanup on process exit
process.on("exit", () => {
  for (const stream of activeLoggers.values()) {
    try {
      stream.end();
    } catch {}
  }
});

/**
 * Create a debug file logger with consistent formatting
 *
 * Features:
 * - File-based logging to configurable path
 * - Optional enable/disable via environment variable
 * - Session start/end markers
 * - Consistent timestamp formatting (HH:MM:SS.mmm)
 * - Automatic directory creation
 *
 * @example
 * ```ts
 * const logger = createDebugFileLogger({
 *   filePath: ".sst/debug-workers.log",
 *   envVar: "SST_DEBUG_WORKERS",
 *   sessionName: "WORKERS",
 * });
 *
 * logger.log("REQUEST", { path: "/api/test", elapsed: 50 });
 * // Output: [14:30:45.123] REQUEST path=/api/test elapsed=50
 * ```
 */
export function createDebugFileLogger(
  config: DebugFileLoggerConfig
): DebugFileLogger {
  const { filePath, envVar, sessionName, sessionHeader, width = 80 } = config;

  let stream: fs.WriteStream | null = null;
  let initialized = false;

  function isEnabled(): boolean {
    // If no envVar specified, always enabled
    if (!envVar) return true;
    return process.env[envVar] === "true";
  }

  function initLogFile(): boolean {
    if (initialized) return stream !== null;
    initialized = true;

    if (!isEnabled()) return false;

    try {
      const logDir = path.dirname(filePath);
      if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir, { recursive: true });
      }

      stream = fs.createWriteStream(filePath, { flags: "w" });
      activeLoggers.set(filePath, stream);

      // Write session start marker
      const header = sessionHeader ? ` | ${sessionHeader}` : "";
      stream.write(
        "\n" +
          "=".repeat(width) +
          "\n" +
          `[${sessionName} SESSION START] ${new Date().toISOString()}${header}\n` +
          "=".repeat(width) +
          "\n"
      );

      return true;
    } catch {
      return false;
    }
  }

  function formatTimestamp(): string {
    return new Date().toISOString().slice(11, 23); // HH:MM:SS.mmm
  }

  function formatDetails(details: Record<string, any>): string {
    return Object.entries(details)
      .filter(([_, v]) => v !== undefined)
      .map(([k, v]) => {
        if (typeof v === "object") {
          return `${k}=${JSON.stringify(v)}`;
        }
        return `${k}=${v}`;
      })
      .join(" ");
  }

  return {
    log(action: string, details: Record<string, any> = {}) {
      if (!initLogFile() || !stream) return;

      const timestamp = formatTimestamp();
      const detailStr = formatDetails(details);
      const line = `[${timestamp}] ${action.padEnd(20)} ${detailStr}\n`;

      stream.write(line);
    },

    logRaw(line: string) {
      if (!initLogFile() || !stream) return;
      stream.write(line.endsWith("\n") ? line : line + "\n");
    },

    isEnabled,

    close(summary?: string) {
      if (!stream) return;

      const endMarker =
        "\n" +
        "-".repeat(width) +
        "\n" +
        `[${sessionName} SESSION END] ${new Date().toISOString()}\n` +
        (summary ? summary + "\n" : "") +
        "-".repeat(width) +
        "\n";

      stream.write(endMarker);
      stream.end();
      activeLoggers.delete(filePath);
      stream = null;
    },

    getStream() {
      initLogFile();
      return stream;
    },
  };
}

/**
 * Create a simple console+file logger for debug output
 * Logs to both console and a file with consistent formatting
 */
export interface ConsoleFileLoggerConfig {
  /** Log file path */
  filePath: string;
  /** Prefix for console output (e.g., "[WORKERS]") */
  prefix: string;
  /** Session name for file markers */
  sessionName: string;
}

export interface ConsoleFileLogger {
  /** Log a message to both console and file */
  log: (message: string) => void;
  /** Check if file logging is enabled */
  isEnabled: () => boolean;
  /** Close the file stream */
  close: () => void;
}

/**
 * Create a logger that writes to both console and a file
 * Always writes to console, writes to file when SST_DEBUG_BRIDGE is set
 */
export function createConsoleFileLogger(
  config: ConsoleFileLoggerConfig
): ConsoleFileLogger {
  const { filePath, prefix, sessionName } = config;

  const fileLogger = createDebugFileLogger({
    filePath,
    envVar: "SST_DEBUG_BRIDGE",
    sessionName,
    width: 100,
  });

  return {
    log(message: string) {
      // Always log to console
      console.log(`${prefix} ${message}`);

      // Also log to file if enabled
      if (fileLogger.isEnabled()) {
        const timestamp = new Date().toISOString().slice(11, 23);
        fileLogger.logRaw(`[${timestamp}] ${prefix} ${message}`);
      }
    },

    isEnabled: fileLogger.isEnabled,
    close: () => fileLogger.close(),
  };
}

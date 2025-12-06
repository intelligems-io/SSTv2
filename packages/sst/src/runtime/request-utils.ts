/**
 * Extracts a request path from a Lambda event for logging purposes.
 * Handles API Gateway v1, v2, and other event formats.
 */
export function getRequestPath(event: unknown): string {
  if (!event || typeof event !== "object") {
    return "[unknown]";
  }

  const evt = event as Record<string, unknown>;

  // API Gateway v2 (HTTP API)
  if (typeof evt.rawPath === "string") {
    return evt.rawPath;
  }

  // API Gateway v1 (REST API)
  if (typeof evt.path === "string") {
    return evt.path;
  }

  // Warmup requests
  if ("ding" in evt || "warmer" in evt || evt.__sst_warmup === true) {
    return "[warmup]";
  }

  // SQS, SNS, or other event types - no path
  return "[event]";
}

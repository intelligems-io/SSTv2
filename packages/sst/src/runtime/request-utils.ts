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

/**
 * Extracts correlation ID from headers for request tracing.
 * Checks common correlation header names.
 *
 * Frontend can send: X-Correlation-ID, X-Request-ID, or X-Trace-ID
 */
export function getCorrelationId(event: unknown): string | undefined {
  if (!event || typeof event !== "object") {
    return undefined;
  }

  const evt = event as Record<string, unknown>;

  // Get headers - handle both v1 and v2 API Gateway formats
  let headers: Record<string, string> | undefined;

  // API Gateway v2 uses lowercase headers
  if (evt.headers && typeof evt.headers === "object") {
    headers = evt.headers as Record<string, string>;
  }

  if (!headers) return undefined;

  // Check common correlation header names (case-insensitive)
  const correlationHeaders = [
    "x-correlation-id",
    "x-request-id",
    "x-trace-id",
    "correlation-id",
    "request-id",
  ];

  // Normalize header keys to lowercase for comparison
  const normalizedHeaders: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    normalizedHeaders[key.toLowerCase()] = value;
  }

  for (const headerName of correlationHeaders) {
    const value = normalizedHeaders[headerName];
    if (value) {
      return value;
    }
  }

  return undefined;
}

/**
 * Extracts API Gateway Request ID from requestContext.
 * This ID is visible in browser dev tools (x-amzn-requestid response header).
 */
export function getApiGatewayRequestId(event: unknown): string | undefined {
  if (!event || typeof event !== "object") {
    return undefined;
  }

  const evt = event as Record<string, unknown>;

  // Check requestContext.requestId (both v1 and v2)
  const requestContext = evt.requestContext as Record<string, unknown> | undefined;
  if (requestContext?.requestId && typeof requestContext.requestId === "string") {
    return requestContext.requestId;
  }

  return undefined;
}

/**
 * Extracts HTTP method from event
 */
export function getHttpMethod(event: unknown): string | undefined {
  if (!event || typeof event !== "object") {
    return undefined;
  }

  const evt = event as Record<string, unknown>;

  // API Gateway v2
  const requestContext = evt.requestContext as Record<string, unknown> | undefined;
  if (requestContext?.http && typeof requestContext.http === "object") {
    const http = requestContext.http as Record<string, unknown>;
    if (typeof http.method === "string") {
      return http.method;
    }
  }

  // API Gateway v1
  if (typeof evt.httpMethod === "string") {
    return evt.httpMethod;
  }

  return undefined;
}

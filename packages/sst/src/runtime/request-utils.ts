// Helper to extract request path from event for logging
export function getRequestPath(event: any): string {
  if (!event || typeof event !== 'object') return 'unknown';
  // API Gateway v2 (HTTP API)
  if (event.rawPath) return event.rawPath;
  // API Gateway v1 (REST API)
  if (event.path) return event.path;
  // ALB
  if (event.requestContext?.path) return event.requestContext.path;
  // Warmup request
  if (event.ding || event.warmer || event.__sst_warmup) return '[warmup]';
  return 'unknown';
}

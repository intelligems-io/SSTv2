const THROTTLING_ERROR_NAMES = new Set([
  "ThrottlingException",
  "Throttling",
  "TooManyRequestsException",
  "OperationAbortedException",
  "TimeoutError",
  "NetworkingError",
]);

/**
 * The HTTP status of the response that failed the call, if one arrived.
 *
 * A modeled AWS error carries it on `$metadata`. A response the SDK could not
 * deserialize — an egress proxy's plain-text 503 such as Envoy's
 * "upstream connect error or disconnect/reset before headers" — surfaces as a
 * bare `SyntaxError`, with the raw response only on `$response`.
 */
function failedStatusCode(e: any): number | undefined {
  return e?.$metadata?.httpStatusCode ?? e?.$response?.statusCode;
}

export type AwsRetryReason = "offline" | "throttled" | "server";

/**
 * Why a failed AWS call should be retried, or undefined to fail it now.
 *
 * Every 5xx is transient: either AWS reported a server fault, or something
 * between the CLI and AWS (an HTTP proxy, a corporate gateway) failed to reach
 * it. Without the 5xx rule a single dropped request through a proxy aborts
 * `sst dev` or `sst deploy`, because the proxy's body is not an AWS error the
 * SDK can classify.
 */
export function awsRetryReason(e: any): AwsRetryReason | undefined {
  if (e?.code === "ENOTFOUND") return "offline";
  if (THROTTLING_ERROR_NAMES.has(e?.name)) return "throttled";
  const status = failedStatusCode(e);
  if (status !== undefined && status >= 500 && status <= 599) return "server";
  return undefined;
}

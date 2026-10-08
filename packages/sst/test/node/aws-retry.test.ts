import { expect, test } from "vitest";
import { awsRetryReason } from "../../src/aws-retry.js";

test("retries a proxy's undeserializable 503", () => {
  const e = Object.assign(
    new SyntaxError(`Unexpected token 'u', "upstream c"... is not valid JSON`),
    { $response: { statusCode: 503 } }
  );
  expect(awsRetryReason(e)).toBe("server");
});

test("retries a modeled AWS 5xx", () => {
  const e = { name: "InternalFailure", $metadata: { httpStatusCode: 500 } };
  expect(awsRetryReason(e)).toBe("server");
});

test("retries throttling and offline errors", () => {
  expect(awsRetryReason({ name: "Throttling" })).toBe("throttled");
  expect(awsRetryReason({ code: "ENOTFOUND" })).toBe("offline");
});

test("fails 4xx errors immediately", () => {
  expect(
    awsRetryReason({ name: "AccessDenied", $metadata: { httpStatusCode: 403 } })
  ).toBeUndefined();
  expect(
    awsRetryReason({
      name: "ValidationError",
      $metadata: { httpStatusCode: 400 },
    })
  ).toBeUndefined();
});

test("fails errors that never got a response", () => {
  expect(awsRetryReason(new Error("boom"))).toBeUndefined();
});

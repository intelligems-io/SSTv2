import { describe, expect, it } from "vitest";
import {
  applyDevEnvOverrides,
  devEnvOverrides,
} from "../../src/runtime/dev-env-overrides.js";

describe("dev env overrides", () => {
  it("applies only the listed keys that the dev process sets", () => {
    const env = {
      SST_DEV_ENV_OVERRIDES: "RESOURCE_ENV, FRONTEND_ENV,AI_STACK_ENV",
      RESOURCE_ENV: "prod",
      FRONTEND_ENV: "dev-4",
      AWS_ACCESS_KEY_ID: "laptop-creds",
    };
    expect(devEnvOverrides(env)).toEqual({
      RESOURCE_ENV: "prod",
      FRONTEND_ENV: "dev-4",
    });
  });

  it("overlays the forwarded stub environment without dropping it", () => {
    const forwarded = {
      AWS_ACCESS_KEY_ID: "stub-creds",
      RESOURCE_ENV: "stale",
      STAGE: "christian-local",
    };
    expect(
      applyDevEnvOverrides(forwarded, {
        SST_DEV_ENV_OVERRIDES: "RESOURCE_ENV",
        RESOURCE_ENV: "dev-6",
        AWS_ACCESS_KEY_ID: "laptop-creds",
      })
    ).toEqual({
      AWS_ACCESS_KEY_ID: "stub-creds",
      RESOURCE_ENV: "dev-6",
      STAGE: "christian-local",
    });
  });

  it("is a no-op without the list", () => {
    expect(applyDevEnvOverrides({ A: "1" }, { RESOURCE_ENV: "prod" })).toEqual({
      A: "1",
    });
    expect(applyDevEnvOverrides(undefined, {})).toEqual({});
  });
});

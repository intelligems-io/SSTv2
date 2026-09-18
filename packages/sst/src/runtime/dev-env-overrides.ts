/**
 * Keys of the `sst dev` process environment that override the environment a
 * deployed stub forwards with each invocation.
 *
 * The stub Lambda snapshots ITS OWN `process.env` and sends it along
 * (`support/bridge/live-lambda.ts`), and the local worker runs with exactly
 * that map. So anything a developer wants to change per session — which
 * database, which frontend — either had to be on the stub's function config
 * (a CloudFormation update of every stub each time it changed) or could not
 * reach the handler at all.
 *
 * `SST_DEV_ENV_OVERRIDES=KEY1,KEY2` names the keys the dev process supplies
 * instead. Only listed keys that are set in the dev process are applied; the
 * rest of the stub's environment (AWS credentials, function metadata) stays
 * as forwarded.
 */
export const SST_DEV_ENV_OVERRIDES_KEY = "SST_DEV_ENV_OVERRIDES";

export function devEnvOverrides(
  env: NodeJS.ProcessEnv = process.env
): Record<string, string> {
  const list = env[SST_DEV_ENV_OVERRIDES_KEY];
  if (!list) return {};
  const overrides: Record<string, string> = {};
  for (const key of list.split(",").map((k) => k.trim()).filter(Boolean)) {
    const value = env[key];
    if (value !== undefined) overrides[key] = value;
  }
  return overrides;
}

export function applyDevEnvOverrides(
  forwarded: Record<string, string> | undefined,
  env: NodeJS.ProcessEnv = process.env
): Record<string, string> {
  return { ...(forwarded ?? {}), ...devEnvOverrides(env) };
}

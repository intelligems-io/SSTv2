import path from "path";
import fsSync from "fs";
import {useProject} from "../project.js";
import {lazy} from "../util/lazy.js";
import {Logger} from "../logger.js";

/**
 * Global mono build configuration for SST dev mode.
 *
 * Mono build mode bundles all Lambda handlers into a single file (.mono-build/index.mjs)
 * instead of building each handler individually. This significantly speeds up dev mode
 * by sharing compilation work across all handlers.
 *
 * Detection latches on: the first check that finds the bundle enables mono
 * build for the rest of the session. It deliberately does NOT latch off, so a
 * caller arriving before the bundle has been written cannot disable it.
 */
export const useMonoBuildConfig = lazy(() => {
  const project = useProject();

  const monoBundleDir = path.join(project.paths.root, ".mono-build");
  const monoBundlePath = path.join(monoBundleDir, "index.mjs");

  /**
   * Latches on true, never on false.
   *
   * This was a single `existsSync` at first call, cached for the session by
   * `lazy`. Dev start deletes `.mono-build/index.mjs` and then rebuilds it, so
   * any caller landing in that window — typically an invocation that beat the
   * bundle to disk — pinned this to false for the whole session. Every handler
   * then took the per-function esbuild path, which does not carry the mono
   * build's `external` list, and failed to resolve packages the mono bundle
   * never opens. The bundle finishing changed nothing, because the flag had
   * already been decided, so the session stayed broken until restarted.
   *
   * Re-checking until it is found costs one `existsSync` per call for the few
   * seconds before the bundle lands, and nothing afterwards.
   */
  let enabled = false;

  const isEnabled = (): boolean => {
    if (!enabled && fsSync.existsSync(monoBundlePath)) {
      enabled = true;
      Logger.debug("Mono build mode enabled:", monoBundlePath);
    }
    return enabled;
  };

  isEnabled();

  return {
    /**
     * Whether mono build mode is enabled. Re-checked until the bundle is
     * found, so a check made before it was written does not stick.
     * When true, all Node.js handlers use the shared .mono-build bundle.
     */
    get enabled(): boolean {
      return isEnabled();
    },

    /**
     * The mono bundle directory (.mono-build)
     */
    dir: monoBundleDir,

    /**
     * The mono bundle entry file (.mono-build/index.mjs)
     */
    entryFile: monoBundlePath,

    /**
     * The handler string to use for mono build mode
     */
    handler: "index.handler",

    /**
     * Check if a build output path represents a mono build.
     * This is useful when you have a build result and need to determine its type.
     */
    isMonoBuildPath(buildOut: string): boolean {
      return isEnabled() && buildOut.includes(".mono-build");
    },

    /**
     * Get the pool key for a function based on mono build status.
     * For mono build: shared key (all functions share workers)
     * For non-mono build: per-function key
     */
    getPoolKey(functionID: string, runtime: string, buildOut: string): { key: string; isShared: boolean } {
      if (isEnabled() && buildOut.includes(".mono-build")) {
        return { key: `${runtime}:mono-build`, isShared: true };
      }
      return { key: `${runtime}:${functionID}`, isShared: false };
    },
  };
});

/**
 * Quick check for mono build mode without full config initialization.
 * Use this for simple boolean checks where you don't need the full config.
 */
export function isMonoBuildEnabled(): boolean {
  return useMonoBuildConfig().enabled;
}

/**
 * Get the mono build directory path.
 */
export function getMonoBuildDir(): string {
  return useMonoBuildConfig().dir;
}

/**
 * Check if a path represents a mono build output.
 */
export function isMonoBuildPath(buildOut: string): boolean {
  return useMonoBuildConfig().isMonoBuildPath(buildOut);
}

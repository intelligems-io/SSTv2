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
 * Detection happens once at startup and the result is cached for the session.
 */
export const useMonoBuildConfig = lazy(() => {
  const project = useProject();

  const monoBundleDir = path.join(project.paths.root, ".mono-build");
  const monoBundlePath = path.join(monoBundleDir, "index.mjs");

  // Check once at startup if mono build exists
  const enabled = fsSync.existsSync(monoBundlePath);

  if (enabled) {
    Logger.debug("Mono build mode enabled:", monoBundlePath);
  }

  return {
    /**
     * Whether mono build mode is enabled (detected at startup).
     * When true, all Node.js handlers use the shared .mono-build bundle.
     */
    enabled,

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
      return enabled && buildOut.includes(".mono-build");
    },

    /**
     * Get the pool key for a function based on mono build status.
     * For mono build: shared key (all functions share workers)
     * For non-mono build: per-function key
     */
    getPoolKey(functionID: string, runtime: string, buildOut: string): { key: string; isShared: boolean } {
      if (enabled && buildOut.includes(".mono-build")) {
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

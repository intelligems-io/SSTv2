import {Logger} from "../logger.js";
import path from "path";
import zlib from "zlib";
import fs from "fs/promises";
import {useWatcher} from "../watcher.js";
import {useBus} from "../bus.js";
import crypto from "crypto";
import {useProject} from "../project.js";
import {FunctionProps, useFunctions} from "../constructs/Function.js";
import {useNodeHandler} from "./handlers/node.js";
import {useContainerHandler} from "./handlers/container.js";
import {useDotnetHandler} from "./handlers/dotnet.js";
import {useGoHandler} from "./handlers/go.js";
import {useJavaHandler} from "./handlers/java.js";
import {usePythonHandler} from "./handlers/python.js";
import {useRustHandler} from "./handlers/rust.js";
import {lazy} from "../util/lazy.js";
import {Semaphore} from "../util/semaphore.js";
import {isMonoBuildEnabled, isMonoBuildPath} from "./mono-build-config.js";

// Build concurrency semaphore - only used for actual builds, not mono-build lookups
const buildSemaphore = new Semaphore(
  parseInt(process.env.SST_BUILD_CONCURRENCY || "4", 10)
);

declare module "../bus.js" {
  export interface Events {
    "function.build.started": {
      functionID: string;
    };
    "function.build.success": {
      functionID: string;
      monoBundle?: boolean;
    };
    "function.build.failed": {
      functionID: string;
      errors: string[];
    };
  }
}

interface BuildInput {
  functionID: string;
  mode: "deploy" | "start";
  out: string;
  props: FunctionProps;
}

export interface StartWorkerInput {
  url: string;
  workerID: string;
  functionID: string;
  environment: Record<string, string>;
  out: string;
  handler: string;
  runtime: string;
  /** Whether this worker is using mono build mode */
  isMonoBuild?: boolean;
}

interface ShouldBuildInput {
  file: string;
  functionID: string;
}

export interface RuntimeHandler {
  startWorker: (worker: StartWorkerInput) => Promise<void>;
  stopWorker: (workerID: string) => Promise<void>;
  shouldBuild: (input: ShouldBuildInput) => boolean;
  canHandle: (runtime: string) => boolean;
  build: (input: BuildInput) => Promise<
    | {
    type: "success";
    handler: string;
    sourcemap?: string;
    out?: string; // Optional: if provided, use this instead of artifacts path (for mono-bundle)
  }
    | {
    type: "error";
    errors: string[];
  }
  >;
}

export const useRuntimeHandlers = lazy(() => {
  const handlers: RuntimeHandler[] = [
    useNodeHandler(),
    useGoHandler(),
    useContainerHandler(),
    usePythonHandler(),
    useJavaHandler(),
    useDotnetHandler(),
    useRustHandler(),
  ];
  const project = useProject();
  const bus = useBus();

  const pendingBuilds = new Map<string, any>();

  const result = {
    subscribe: bus.forward("function.build.success", "function.build.failed"),
    register: (handler: RuntimeHandler) => {
      handlers.push(handler);
    },
    for: (runtime: string) => {
      const result = handlers.find((x) => x.canHandle(runtime));
      if (!result) throw new Error(`${runtime} runtime is unsupported`);
      return result;
    },
    async build(functionID: string, mode: BuildInput["mode"]) {
      // Fast path for mono-bundle: check without semaphore since no actual build work
      async function tryMonoBundleFastPath() {
        const func = useFunctions().fromID(functionID);
        if (!func) return null;

        const handler = result.for(func.runtime!);
        const out = path.join(project.paths.artifacts, functionID);

        // Check for mono-bundle mode by doing a preliminary build call
        // In mono-bundle mode, handler returns its own out path immediately without building
        const monoBundleCheck = await handler!.build({
          functionID,
          out,
          mode,
          props: func,
        });

        // If mono-bundle detected (handler returned custom out), skip all artifact work
        // Don't fire build events - mono-bundle is built externally by esbuild watch
        // Worker pool invalidation is handled separately when bundle file actually changes
        if (monoBundleCheck.type === "success" && monoBundleCheck.out) {
          return {
            type: "success" as const,
            handler: monoBundleCheck.handler,
            out: monoBundleCheck.out,
            sourcemap: monoBundleCheck.sourcemap,
          };
        }

        return null; // Not mono-bundle, need full build
      }

      // Full build with semaphore protection for actual compilation work
      async function fullBuild() {
        const func = useFunctions().fromID(functionID);
        if (!func)
          return {
            type: "error" as const,
            errors: [`Function with ID "${functionID}" not found`],
          };
        const handler = result.for(func.runtime!);
        const out = path.join(project.paths.artifacts, functionID);

        // Acquire semaphore only for actual build work
        const unlock = await buildSemaphore.lock();
        try {
          // Non-mono-bundle: follow original flow
          await fs.rm(out, {recursive: true, force: true});
          await fs.mkdir(out, {recursive: true});

          bus.publish("function.build.started", {functionID});

          if (func.hooks?.beforeBuild) await func.hooks.beforeBuild(func, out);
          const built = await handler!.build({
            functionID,
            out,
            mode,
            props: func,
          });
          if (built.type === "error") {
            bus.publish("function.build.failed", {
              functionID,
              errors: built.errors,
            });
            return built;
          }
          if (func.copyFiles) {
            await Promise.all(
              func.copyFiles.map(async (entry) => {
                const fromPath = path.join(project.paths.root, entry.from);
                const to = entry.to || entry.from;
                if (path.isAbsolute(to))
                  throw new Error(
                    `Copy destination path "${to}" must be relative`
                  );
                const toPath = path.join(out, to);
                if (mode === "deploy")
                  await fs.cp(fromPath, toPath, {
                    recursive: true,
                  });
                if (mode === "start") {
                  try {
                    const dir = path.dirname(toPath);
                    await fs.mkdir(dir, {recursive: true});
                    await fs.symlink(fromPath, toPath);
                  } catch (ex) {
                    Logger.debug("Failed to symlink", fromPath, toPath, ex);
                  }
                }
              })
            );
          }

          if (func.hooks?.afterBuild) await func.hooks.afterBuild(func, out);

          bus.publish("function.build.success", {functionID});
          return {
            ...built,
            out,
            sourcemap: built.sourcemap,
          };
        } finally {
          unlock();
        }
      }

      async function task() {
        // Try mono-bundle fast path first (no semaphore needed)
        const monoBundleResult = await tryMonoBundleFastPath();
        if (monoBundleResult) {
          return monoBundleResult;
        }

        // Fall back to full build with semaphore protection
        return fullBuild();
      }

      if (pendingBuilds.has(functionID)) {
        Logger.debug("Waiting on pending build", functionID);
        return pendingBuilds.get(functionID)! as ReturnType<typeof task>;
      }
      const promise = task();
      pendingBuilds.set(functionID, promise);
      Logger.debug("Building function", functionID);
      try {
        return await promise;
      } finally {
        pendingBuilds.delete(functionID);
      }
    },
  };

  return result;
});

interface Artifact {
  out: string;
  handler: string;
}

export const useFunctionBuilder = lazy(() => {
  const artifacts = new Map<string, Artifact>();
  const handlers = useRuntimeHandlers();

  // Track pending builds to prevent duplicate concurrent builds for same function
  const pendingArtifactBuilds = new Map<string, Promise<Artifact | undefined>>();

  const result = {
    artifact: (functionID: string) => {
      // Fast path: already cached - return immediately without any async work
      if (artifacts.has(functionID)) return artifacts.get(functionID)!;
      return result.build(functionID);
    },
    build: async (functionID: string): Promise<Artifact | undefined> => {
      // Fast path: already cached (check again in case of concurrent calls)
      if (artifacts.has(functionID)) return artifacts.get(functionID)!;

      // Deduplication: if build already in progress for this function, wait for it
      const pending = pendingArtifactBuilds.get(functionID);
      if (pending) return pending;

      const buildTask = async (): Promise<Artifact | undefined> => {
        try {
          // handlers.build() handles semaphore internally:
          // - mono-build: no semaphore (fast path)
          // - non-mono-build: semaphore protected
          const buildResult = await handlers.build(functionID, "start");
          if (!buildResult) return;
          if (buildResult.type === "error") return;
          artifacts.set(functionID, buildResult);
          return artifacts.get(functionID)!;
        } finally {
          pendingArtifactBuilds.delete(functionID);
        }
      };

      const promise = buildTask();
      pendingArtifactBuilds.set(functionID, promise);
      return promise;
    },
  };

  const watcher = useWatcher();
  watcher.subscribe("file.changed", async (evt) => {
    try {
      const functions = useFunctions();
      for (const [functionID, info] of Object.entries(functions.all)) {
        // Optimization: For mono-build, the artifact path is stable and build is handled externally.
        // We can skip the potentially expensive shouldBuild check and rebuild call.
        const existing = artifacts.get(functionID);
        if (existing && isMonoBuildPath(existing.out)) continue;

        const handler = handlers.for(info.runtime!);
        if (
          !handler?.shouldBuild({
            functionID,
            file: evt.properties.file,
          })
        )
          continue;
        await result.build(functionID);
        Logger.debug("Rebuilt function", functionID);
      }
    } catch {
    }
  });

  return result;
});

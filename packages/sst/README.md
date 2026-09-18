# sst

[SST](https://sst.dev) makes it easy to build modern full-stack applications on AWS.

The `sst` package is made up of the following.

- [`sst`](https://docs.sst.dev/packages/sst) CLI
- [`sst/node`](https://docs.sst.dev/clients) Node.js client
- [`sst/constructs`](https://docs.sst.dev/constructs) CDK constructs

## Installation

Install the `sst` package in your project root.

```bash
npm install sst --save-exact
```

## Usage

Once installed, you can run the CLI commands using.

```bash
npx sst <command>
```

Import the Node.js client in your functions. For example, you can import the `Bucket` client.

```ts
import { Bucket } from "sst/node/bucket";
```

And import the constructs you need in your stacks code. For example, you can add an API.

```ts
import { Api } from "sst/constructs";
```

For more details, [head over to our docs](https://docs.sst.dev).

---

**Join our community** [Discord](https://sst.dev/discord) | [YouTube](https://www.youtube.com/c/sst-dev) | [Twitter](https://twitter.com/SST_dev)

## Dev mode tuning (Intelligems fork)

`sst dev` runs Node functions as worker threads inside the CLI process. In mono-build mode every worker loads
the whole `.mono-build` bundle, so the number of live workers is what decides memory use. These variables
control it (defaults in `src/runtime/worker-config.ts`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `SST_WORKER_POOL_SIZE` | `4` | Max live workers per pool (one shared pool for all mono-build Node functions). Requests beyond that wait for a free worker instead of spawning a new one. |
| `SST_WORKER_CONCURRENCY` | `5` | Invocations one Node worker runs at the same time. Each invocation gets its own `process.env`. Set to `1` to fall back to one request per worker. |
| `SST_WORKER_IDLE_TIMEOUT` | `300000` | Milliseconds an idle worker is kept before it is terminated. |
| `SST_WARMUP_COUNT` | pool size | Warm pings sent at startup (capped at the pool size). `0` disables warmup. |
| `SST_WORKER_MAX_HEAP_MB` | `1024` | V8 old-space cap per worker. A worker that exceeds it exits and its in-flight requests fail; the dev session keeps running. `0` removes the cap. |
| `SST_SOURCE_MAPS` | unset | `true` runs workers with `--enable-source-maps` (costs memory per worker). |
| `SST_DEBUG_MEMORY` | unset | `true` samples process and worker memory to `.sst/memory.log` every 10s and adds peaks to the pool session summary. |
| `SST_DEBUG_POOL` | unset | `true` logs pool events (`CREATE`, `REUSE`, `POOL_WAIT`, `TERMINATE`, …) to `.sst/worker-pool.log`. |

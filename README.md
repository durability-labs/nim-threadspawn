# threadspawn

Cancellation-safe primitives for bridging taskpools thread spawn to Chronos
async, extracted from archivist-node.

Three composable layers:

- `awaitSpawn` — lowest-level primitive: cancellation-safe wait on a
  `ThreadSignalPtr` future, with optional `onError` cleanup and a noCancel
  drain that lets the worker finish before the caller returns (preventing
  use-after-free of heap-allocated task data).
- `withThreadSignal` — template providing raw signal lifecycle management
  (create + defer-close) for custom task structs.
- `spawnJoin[T]` — high-level primitive encapsulating the full pattern:
  signal create/close, `SharedPtr[TaskCtx[T]]` context, worker spawn,
  cancellation-safe await, and isolate/move result extraction.

Workers write results into `ctx[].result` via `unsafeIsolate(move result)`
with `ThreadSpawnRes[T]` (`Result[T, string]`); `mapThreadSpawnErr` converts
typed results at the worker boundary and `mapSpawnFailure`/`SpawnFailure`
convert thread results to `?!T` at the async boundary.

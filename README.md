# threadspawn

Cancellation-safe primitives for bridging taskpools thread spawn to Chronos
async. Dual-licensed Apache-2.0 / MIT, Copyright (c) 2026 Archivist
Authors. Requires `-threads:on` (a compile-time error is raised otherwise).

## What it solves

Two pain points at the spawn/await boundary:

1. **Signal lifecycle boilerplate.** Every call site repeats the
   `ThreadSignalPtr.new` / close / wait / cancellation-safe-join dance.
   `withThreadSignal` handles signal create + defer-close;
   `awaitSpawn` handles the cancellation-safe join.
2. **Result transfer.** Workers write results into `ctx[].result` via
   `unsafeIsolate(move result)`; `spawnJoin` extracts after awaiting.
   `TaskCtx[T]` is passed by `SharedPtr`, cleaned up via atomic refcounting.

## Three composable layers

### `awaitSpawn` - lowest level

```nim
proc awaitSpawn*(taskFut: SpawnFut, onError: OnSpawnError = nil): Future[?!void]
```

Cancellation-safe wait on the future returned by `ThreadSignalPtr.wait`.
Uses `join()` + `noCancel`: cancelling the caller does **not** cancel the
worker - it runs to completion before returning, preventing use-after-free
of heap-allocated task data. On cancellation, `CancelledError` is re-raised
after the worker finishes. `onError` (must be `raises: []` - a raising
cleanup would trip chronos' `noCancel` `raiseAssert`) runs before the drain.
Signal lifecycle is the caller's responsibility.

### `withThreadSignal` - raw signal management

```nim
template withThreadSignal*(signalName, body: untyped)
```

Creates a `ThreadSignalPtr`, injects it as `signalName` (caller-chosen, no
identifier collisions), and runs `body` in a block with `signal.close`
deferred - on all exit paths, including exceptions and cancellation.
Close errors are warn-only (the signal is being destroyed).

### `spawnJoin[T]` - full pattern, one call

```nim
proc spawnJoin*[T](spawnFn: SpawnFn[T], onError: OnSpawnError = nil): Future[?!T]

type SpawnFn*[T] = proc(ctx: SharedPtr[TaskCtx[T]]) {.gcsafe, raises: [].}
```

Encapsulates signal create/close, `SharedPtr[TaskCtx[T]]`, worker spawn,
cancellation-safe await, and result extraction. The spawn callback must be
`raises: []` (an exception after enqueuing would unwind the signal-close
defer while the queued worker still holds the signal). Takes a spawn
callback, not a `Taskpool`, so any backend that can run a proc and fire a
`ThreadSignalPtr` works.

```nim
let value = ?await spawnJoin[seq[Key]]:
  proc(ctx: SharedPtr[TaskCtx[seq[Key]]]) {.gcsafe, raises: [].} =
    tp.spawn runHasTaskMany(ctx, paths)
```

Workers write results with `ctx[].result = unsafeIsolate(move result)` and
fire the signal. For parallel loops, use `withThreadSignal` per spawn,
collect the futures, and `awaitSpawn` each.

## Result channel

- `ThreadSpawnRes*[T] = Result[T, string]` - the error is an owned string
  so the message stays valid after the worker's scope ends.
- `TaskCtx*[T]` - `signal` (`ThreadSignalPtr`) + `result`
  (`Isolated[ThreadSpawnRes[T]]`).
- `mapThreadSpawnErr*[T, V](exp: Result[T, V]): ThreadSpawnRes[T]` -
  converts typed results at the worker boundary (message copied into an
  owned string).
- `mapSpawnFailure*[T](res): Result[T, ref CatchableError]` - converts
  thread results to `?!T` at the async boundary, wrapping failures in
  `SpawnFailure`.

## The GC-ref constraint

`ThreadSpawnRes[T]` payloads must not transitively contain GC-managed
references: the result crosses a thread boundary and the worker's heap may
be torn down before the caller reads it. Enforced at compile time by
`assertNoGCRefs[T]()` (backed by the `containsGCRef` AST walker), invoked
from both `mapThreadSpawnErr` (worker boundary) and `spawnJoin`
(extraction). Direct `withThreadSignal` + `awaitSpawn` users that read
`ctx[].result` manually are on their own.

Allowed:

- `ptr` / `pointer` - plain values; the pointed-to data stays the caller's
  responsibility
- non-capturing procs (`nimcall` and friends) - closures are forbidden
- `{.acyclic.}` ref types - ORC never enters them in the per-thread cycle
  registry; the pointee is walked transitively (nested non-acyclic refs are
  still rejected), and ownership must be unique at transfer (refcounts are
  not atomic in this build - the worker must not retain a reference after
  writing the result)

Forbidden: `ref` types (anonymous or named, unless `{.acyclic.}`),
closures, closure iterators.

## Install

```bash
nimble install https://github.com/durability-labs/threadspawn@#main
```

Requires Nim >= 2.0.14, chronos 4.0.x, chronicles 0.12.x, questionable
0.10.x, taskpools >= 0.0.5, threading 0.2.x. Tests: `nimble test`.

# threadspawn

Cancellation-safe primitives for bridging taskpools thread spawn to Chronos async. Requires `-threads:on`.

## What it solves

Two pain points at the spawn/await boundary:

1. **Signal lifecycle boilerplate.** Every call site repeats the `ThreadSignalPtr.new` / close / wait / cancellation-safe-join dance. `withThreadSignal` handles signal create + defer-close; `awaitSpawn` handles the cancellation-safe join.
2. **Result transfer.** Workers write results into `ctx[].result` via the `ThreadSpawnRes` constructors (`ok`/`err`); `spawnJoin` extracts after awaiting. `TaskCtx[T, E]` is passed by `SharedPtr`, cleaned up via atomic refcounting.

## Three composable layers

### `awaitSpawn` - lowest level

```nim
proc awaitSpawn*(taskFut: SpawnFut, onError: OnSpawnError = nil): Future[?!void]
```

Cancellation-safe wait on the future returned by `ThreadSignalPtr.wait`. Uses `join()` + `noCancel`: cancelling the caller does **not** cancel the worker - it runs to completion before returning, preventing use-after-free of heap-allocated task data. On cancellation, `CancelledError` is re-raised after the worker finishes. `onError` (must be `raises: []` - a raising cleanup would trip chronos' `noCancel` `raiseAssert`) runs before the drain. Signal lifecycle is the caller's responsibility.

### `withThreadSignal` - raw signal management

```nim
template withThreadSignal*(signalName, body: untyped)
```

Creates a `ThreadSignalPtr`, injects it as `signalName` (caller-chosen, no identifier collisions), and runs `body` in a block with `signal.close` deferred - on all exit paths, including exceptions and cancellation. Close errors are warn-only (the signal is being destroyed).

### `spawnJoin[T, E]` - full pattern, one call

```nim
proc spawnJoin*[T, E](
    spawnFn: SpawnFn[T, E],
    onError: OnSpawnError = nil,
    errMap: proc(e: E): ref CatchableError {.gcsafe, raises: [].} = nil,
): Future[?!T]

type SpawnFn*[T, E = string] = proc(ctx: SharedPtr[TaskCtx[T, E]]) {.gcsafe, raises: [].}
```

Encapsulates signal create/close, `SharedPtr[TaskCtx[T, E]]`, worker spawn, cancellation-safe await, and result extraction. The spawn callback must be `raises: []` (an exception after enqueuing would unwind the signal-close defer while the queued worker still holds the signal). Takes a spawn callback, not a `Taskpool`, so any backend that can run a proc and fire a `ThreadSignalPtr` works. `errMap` converts the worker's typed error `E` to the `?!T` error type; when nil, failures map to `SpawnFailure` via `$e`. `E` is inferred from the worker's ctx type (`TaskCtx[seq[Key]]` infers `E = string`); for a typed error, pass both explicitly: `spawnJoin[T, MyErr](...)`.

```nim
let value = ?await spawnJoin:
  proc(ctx: SharedPtr[TaskCtx[seq[Key]]]) {.gcsafe, raises: [].} =
    tp.spawn runHasTaskMany(ctx, paths)
```

Workers write results with `ctx[].result = ThreadSpawnRes[T].ok(...)` and fire the signal. For parallel loops, use `withThreadSignal` per spawn, collect the futures, and `awaitSpawn` each.

## Result channel

- `ThreadSpawnRes*[T, E = string] = Isolated[Result[T, E]]` - move-only; the payload and error are checked by the compiler's `Isolate` mechanism at construction.
- `TaskCtx*[T, E = string]` - `signal` (`ThreadSignalPtr`) + `result` (`ThreadSpawnRes[T, E]`).
- `mapThreadSpawnErr*[T, V](exp: Result[T, V]): ThreadSpawnRes[T, string]` - converts typed results at the worker boundary, isolating the result (message copied into an owned string).
- `spawnJoin`'s `errMap` converts thread results to `?!T` at the async boundary; the default wraps failures in `SpawnFailure`.

## The GC-ref constraint

`ThreadSpawnRes[T, E]` payloads and errors must not transitively contain non-acyclic GC-managed references: the result crosses a thread boundary and the worker's heap may be torn down before the caller reads it.

The result type is `Isolated[Result[T, E]]`, and construction through the `ok`/`err` constructors runs the compiler's `Isolate` check at the assignment site: it proves the payload expression is unshared and fails the build otherwise. The check cannot see through sequences: refs nested inside seqs pass and must be `{.acyclic.}` - ORC never enters acyclic types in the per-thread cycle registry, so a worker-created acyclic ref crosses safely under unique ownership (the worker must not retain a reference after writing the result; refcounts are not atomic in this build). Direct `withThreadSignal` + `awaitSpawn` users that read `ctx[].result` bypass the constructor check and are responsible for the same guarantees themselves.

Allowed:

- `ptr` / `pointer` - plain values; the pointed-to data stays the caller's responsibility
- non-capturing procs (`nimcall` and friends) - closures are forbidden
- `{.acyclic.}` ref types - when the analysis proves the value unshared (fresh construction)

Forbidden: non-acyclic (`cycle-tracked`) `ref` types, closures, closure iterators.

## Install

```bash
nimble install https://github.com/durability-labs/threadspawn@#main
```

Requires Nim >= 2.0.14, chronos 4.0.x, chronicles 0.12.x, questionable 0.10.x, taskpools >= 0.0.5, threading 0.2.x. Tests: `nimble test`.

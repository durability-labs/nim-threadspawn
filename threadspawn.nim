## Copyright (c) 2026 Archivist Authors
## Licensed under either of
##  * Apache License, version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

## Convenience wrappers for bridging taskpools thread spawn to Chronos async.
##
## The two pain points these wrappers eliminate:
##
## 1. **Signal lifecycle boilerplate.** Every call site repeats the same
##    ThreadSignalPtr.new / defer-close / wait / cancellation-safe-join dance.
##    ``withThreadSignal`` handles signal create+destroy; ``awaitSpawn`` handles
##    the cancellation-safe join.
## 2. **Result transfer.** Workers write results into ``ctx[].result`` via
##    the ``ThreadSpawnRes`` constructors (``ThreadSpawnRes[T].ok(...)``)
##    and ``spawnJoin`` extracts after awaiting.  ``TaskCtx[T, E]`` uses a
##    ``SharedPtr`` so the ctx can be passed to the worker by pointer and
##    cleaned up via atomic refcounting.
##
## Usage by pattern:
##
## **Custom task struct** (erasure, nimgroth16, nat-traversal):
##
##   .. code-block:: nim
##      withThreadSignal(sig):
##        var task = MyTask(data: data, signal: sig)
##        tp.spawn myWorker(addr task)
##        ?await awaitSpawn(sig.wait())
##        if not task.success.load: return failure("task failed")
##
## **Generic result** (kvstore-style):
##
##   .. code-block:: nim
##      let value = ?await spawnJoin:
##        proc(ctx: SharedPtr[TaskCtx[MyResultType]]) {.gcsafe.} =
##          tp.spawn myWorker(ctx, args)
##
## Backend abstraction: ``spawnJoin`` takes a spawn callback, not a
## ``Taskpool``, so any backend that can run a proc and fire a
## ``ThreadSignalPtr`` works.  Only ``awaitSpawn`` uses chronos internals
## (``join`` / ``noCancel``), which are backend-independent.
##
## Multi-spawn note: ``spawnJoin`` covers both the single-spawn-then-await
## majority and parallel loops - spawn once per item and await each (e.g.
## kvstore ``getImpl`` chunked reads).  Each ``spawnJoin`` owns its signal
## lifecycle across its own suspension.  Do NOT share one ``withThreadSignal``
## block across spawns whose awaits outlive the block: its block-scoped
## ``defer: signal.close()`` closes the fd while a worker still holds it.
## For long-lived per-task signals (archivist asyncbuilder's batch pattern),
## own the signal in the task context and close it after the awaited join.

{.push raises: [].}

when not compileOption("threads"):
  {.error: "threadspawn requires -threads:on".}

import std/isolation

import pkg/chronos
import pkg/chronos/threadsync
import pkg/chronicles
import pkg/questionable/results
import pkg/threading/smartptrs

export isolation except unsafeIsolate
export threadsync
export smartptrs

logScope:
  topics = "archivist threadspawn"

type
  SpawnFut* = Future[void].Raising([AsyncError, CancelledError])
    ## The future returned by ``ThreadSignalPtr.wait``.

  ThreadSpawnRes*[T, E = string] = Isolated[Result[T, E]]
    ## Thread safe result type. Values are move-only (`=copy` is an error on
    ## Isolated) and consumed exactly once via `extract`. Construction through
    ## `ok`/`err` runs the compiler's `Isolate` check: T and E must not be
    ## references, closures, or contain them in object fields, arrays, or
    ## tuples. Refs nested inside seqs are not visible to that analysis and
    ## must be `{.acyclic.}` (never entered in ORC's per-thread cycle
    ## registry); `ptr`/`pointer` and `{.nimcall.}` procs are plain values.
    ##
    ## Workers write `ctx[].result = ThreadSpawnRes[T].ok(...)`; the caller
    ## extracts with `extract(ctx[].result)`.

  TaskCtx*[T, E = string] = object
    ## Per-task state for cross-thread communication with generic results.
    ##
    ## ``signal``: completion notification (fired by the worker via
    ## ``fireSync``).
    ## ``result``: output value wrapped in ``ThreadSpawnRes``, which is
    ## `Isolated[Result[T, E]]` - workers must construct it via the `ok`/`err`
    ## constructors, which run the `Isolate` payload check.
    ##
    ## Memory management: use ``SharedPtr[TaskCtx[T, E]]`` so atomic refcounting
    ## handles cleanup.  The ``SharedPtr`` passes through ``toTask``'s
    ## ``isolate`` as a single pointer move.
    signal*: ThreadSignalPtr
    result*: ThreadSpawnRes[T, E]

  SpawnFailure* = object of CatchableError
    ## Error type used when converting non-exception errors to ?!T.

  SpawnError*[E = string] = object of CatchableError
    ## Base error type of the spawn channel.  ``error`` holds the actual
    ## error value untouched; being a ``CatchableError`` it composes with
    ## ``?!T``-style error handling through the exception hierarchy.
    error*: E

  SpawnContractError* = object of CatchableError
    ## Contract failure: signal creation, wait registration, drain errors.
    ## Message-only.  Concrete (non-generic) so it can appear in ``raises``
    ## annotations - the async macro rejects generic instantiations there.

  SpawnUserError*[E = string] = ref SpawnError[E]
    ## Worker error envelope: the worker's typed error ``E`` is preserved
    ## in ``error``.  Never raised - carried in the result channel.

  SpawnFn*[T, E = string] = proc(ctx: SharedPtr[TaskCtx[T, E]]) {.gcsafe, raises: [].}
    ## Callback that receives the ctx and spawns the worker.
    ## Must call ``tp.spawn worker(ctx, ...)`` inside.
    ##
    ## ``raises: []`` prevents a callback exception from unwinding ``spawnJoin``'s
    ## signal-close defer while the queued worker still holds the signal.

proc toSpawnError*[E](error: E): SpawnUserError[E] {.gcsafe, raises: [].} =
  ## Envelope a worker error into the spawn channel error type.  The error
  ## value is preserved in ``error``; ``msg`` mirrors it for logging.
  when E is (ref Exception):
    (ref SpawnError[E])(msg: error.msg, error: error)
  else:
    mixin `$`
    (ref SpawnError[E])(msg: $error, error: error)

proc ok*[T, E](R: type ThreadSpawnRes[T, E], value: sink T): R =
  ## Wrap a worker success value. Runs the `Isolate` payload check.
  isolate(Result[T, E].ok(move value))

proc ok*[E](R: type ThreadSpawnRes[void, E]): R =
  ## Wrap a worker success for a void task.
  isolate(Result[void, E].ok())

proc err*[T, E](R: type ThreadSpawnRes[T, E], error: sink E): R =
  ## Wrap a worker failure. Runs the `Isolate` payload check on E.
  isolate(Result[T, E].err(move error))

proc err*[E](R: type ThreadSpawnRes[void, E], error: sink E): R =
  ## Wrap a worker failure for a void task.
  isolate(Result[void, E].err(move error))

template mapThreadSpawnErr*[T, V](exp: Result[T, V]): ThreadSpawnRes[T, string] =
  ## Convert `Result[T, E]` to `ThreadSpawnRes[T, string]` at the worker
  ## boundary.
  ##
  ## The message is copied into an owned string, so it stays valid after the
  ## worker's scope ends. The result is isolated here, so callers must not
  ## wrap it again.

  isolate exp.mapErr(
    proc(e: V): string =
      when typeof(e) is (ref Exception):
        e.msg
      else:
        mixin `$`
        $e
  )

template mapThreadSpawnFailure*[T, V](
    exp: Result[T, V]
): Result[T, ref CatchableError] =
  ## Convert `Result[T, E]` to a `ref CatchableError` result at the caller
  ## boundary: exception errors pass through, everything else is wrapped in
  ## `SpawnFailure`.

  exp.mapErr(
    proc(e: V): ref CatchableError =
      when typeof(e) is (ref CatchableError):
        (ref CatchableError)(e)
      else:
        mixin `$`
        newException(SpawnFailure, $e)
  )

proc awaitSpawn*(
    taskFut: SpawnFut
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Cancellation-safe wait for a spawned task to complete.
  ##
  ## ``taskFut`` is the future returned by ``ThreadSignalPtr.wait``.
  ## Uses ``join()`` + ``noCancel`` so that cancelling this future does NOT
  ## cancel the underlying worker - the worker runs to completion before we
  ## return, preventing use-after-free of heap-allocated task data.
  ##
  ## On cancellation: re-raises ``CancelledError`` after the worker finishes.
  ## On contract errors (wait registration, drain): returns the failure
  ## wrapped in ``SpawnFailure``.
  ##
  ## Signal lifecycle is NOT handled here - the caller (typically via
  ## ``withThreadSignal`` or ``spawnJoin``) owns ``signal.close`` via defer.
  ##

  let joinRes = catch(await taskFut.join())
  if joinErr =? joinRes.errorOption:
    # Must wait for worker to finish before returning - without this the
    # caller could free heap-allocated ctx/task while the worker is still
    # writing to it.
    let drainRes = catch(await noCancel taskFut)
    if drainErr =? drainRes.errorOption:
      return failure(newException(SpawnFailure, drainErr.msg))
    if joinErr of CancelledError:
      raise (ref CancelledError)(joinErr)

    return failure(newException(SpawnFailure, joinErr.msg))

  # join() completes unconditionally and does not forward the source future's
  # error.  If taskFut itself failed (e.g. register2/addReader2 in
  # ThreadSignalPtr.wait), return the error rather than reporting success
  # with an unwritten result.
  if taskFut.failed():
    return failure(newException(SpawnFailure, taskFut.error().msg))

  success()

{.pop.} # templates splice into caller scope; their raises are checked there

template withThreadSignal*(signalName, body: untyped) =
  ## Create a ``ThreadSignalPtr``, bind it to ``signalName``, and run ``body``
  ## inside a ``block`` with ``signal.close`` deferred.
  ##
  ## Designed for custom task structs where the caller reads typed fields
  ## (e.g. ``Atomic[bool]``) after awaiting.  Everything - spawn, await, and
  ## result extraction - lives inside the block, so task variables are
  ## properly scoped and don't leak into the enclosing proc.
  ##
  ## ``signalName`` is caller-chosen so it cannot collide with existing variables.
  ##
  ## On signal creation failure, raises ``SpawnContractError``.
  ##
  ##   .. code-block:: nim
  ##      withThreadSignal(sig):
  ##        var task = MyTask(signal: sig, ...)
  ##        tp.spawn myWorker(addr task)
  ##        ?await awaitSpawn(sig.wait())
  ##        if not task.ok.load: return failure("task failed")
  ##
  ## The defer runs on ALL exit paths (early return, exception propagation),
  ## so the signal fd is never leaked - even on cancellation.
  ##
  block:
    let signalRes = ThreadSignalPtr.new().mapThreadSpawnFailure
    if sigErr =? signalRes.errorOption:
      raise (ref SpawnContractError)(msg: sigErr.msg)
    let signalName {.inject.} = signalRes.unsafeGet

    defer:
      let closeRes = signalName.close()
      if closeRes.isErr:
        warn "Failed to close thread signal", error = closeRes.error

    body

proc spawnJoin*[T, E](
    spawnFn: SpawnFn[T, E]
): Future[Result[T, SpawnUserError[E]]] {.
    async: (raises: [CancelledError, SpawnContractError])
.} =
  ## Create a ``ThreadSignalPtr`` + ``SharedPtr[TaskCtx[T, E]]``, spawn the worker,
  ## await completion, close the signal, and extract the result.
  ##
  ## Returns the worker's extracted result; worker failures are enveloped
  ## in ``SpawnUserError[E]`` (the typed error is preserved in ``error``),
  ## which composes with ``?!T``-style handlers through the
  ## ``CatchableError`` hierarchy.
  ##
  ## Contract failures (signal creation, wait registration, drain errors)
  ## raise ``SpawnContractError``; cancellation propagates as
  ## ``CancelledError``.
  ##
  ## ``spawnFn`` receives the ctx and must spawn the worker (e.g.
  ## ``tp.spawn worker(ctx, ...)``).  The callback runs on the calling thread
  ## before the first await, so any backend that can run a proc and fire a
  ## ``ThreadSignalPtr`` works.  Workers write results with:
  ## ``ctx[].result = ThreadSpawnRes[T].ok(...)``
  ##
  ## ``E`` is inferred from the worker's ctx type (e.g. ``TaskCtx[seq[Key]]``
  ## infers ``E = string``); for a typed error, pass it explicitly:
  ## ``spawnJoin[T, MyErr](...)``.
  ##
  ## Signal lifecycle (create/close) is fully encapsulated - the caller never
  ## touches ``ThreadSignalPtr``.  The defer runs on ALL exit paths, so the
  ## signal fd is never leaked.
  ##
  ## Example:
  ##
  ##   .. code-block:: nim
  ##      let keys = ?await spawnJoin:
  ##        proc(ctx: SharedPtr[TaskCtx[seq[Key]]]) {.gcsafe.} =
  ##          self.tp.spawn runHasTaskMany(ctx, paths)

  withThreadSignal(sig):
    let
      ctx = newSharedPtr(TaskCtx[T, E](signal: sig))
      taskFut = sig.wait()

    if taskFut.failed():
      raise (ref SpawnContractError)(msg: taskFut.error().msg)

    spawnFn(ctx)
    let awaited = await awaitSpawn(taskFut)
    if awaitErr =? awaited.errorOption:
      raise (ref SpawnContractError)(msg: awaitErr.msg)

    extract(ctx[].result).mapErr(
      proc(e: E): SpawnUserError[E] =
        toSpawnError(e)
    )

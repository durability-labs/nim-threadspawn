import std/atomics
import std/options

import pkg/chronos
import pkg/chronicles
import pkg/taskpools
import pkg/questionable/results

import pkg/threadspawn

import pkg/asynctest/chronos/unittest2

type ToyTask = object
  value: int
  ok: Atomic[bool]
  signal: ThreadSignalPtr

type AcyclicBlock {.acyclic.} = ref object of RootObj
  cid: int
  data: seq[byte]

type CyclicNode = ref object
  next: CyclicNode

type TaskErr = object
  code: int
  msg: string

proc `$`(e: TaskErr): string =
  e.msg

static:
  # The constructors run the compiler's Isolate check: cycle-tracked refs
  # and closures must not compile as payloads; values, seqs, and void must.
  doAssert not compiles(ThreadSpawnRes[CyclicNode].ok(CyclicNode()))
  doAssert not compiles(
    ThreadSpawnRes[proc() {.closure.}].ok(cast[proc() {.closure.}](nil))
  )
  doAssert compiles(ThreadSpawnRes[seq[byte]].ok(@[1'u8]))
  doAssert compiles(ThreadSpawnRes[void].ok())
  doAssert compiles(ThreadSpawnRes[void].err("boom"))

proc toyWorker(task: ptr ToyTask) {.gcsafe.} =
  task[].ok.store(true)
  discard task[].signal.fireSync()

proc toyFailingWorker(task: ptr ToyTask) {.gcsafe.} =
  task[].ok.store(false)
  discard task[].signal.fireSync()

proc toySlowWorker(task: ptr ToyTask) {.gcsafe.} =
  # Write to the atomic on each iteration so the optimizer cannot remove
  # the loop.  The loop count is large enough to keep the worker running
  # while the awaiter cancels.
  for i in 0 ..< 100_000_000:
    task[].ok.store(false)
  task[].ok.store(true)
  discard task[].signal.fireSync()

suite "threadspawn wrappers":
  var tp: Taskpool

  setup:
    tp = Taskpool.new(numThreads = 2)

  teardown:
    tp.shutdown()

  test "withThreadSignal + awaitSpawn returns on success":
    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      withThreadSignal(sig):
        var task = ToyTask(value: 42, signal: sig)
        tp.spawn toyWorker(addr task)
        ?await awaitSpawn(sig.wait())
        check task.ok.load()
      success()

    (await runTest()).tryGet()

  test "withThreadSignal + awaitSpawn propagates failure flag":
    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      withThreadSignal(sig):
        var task = ToyTask(value: 42, signal: sig)
        tp.spawn toyFailingWorker(addr task)
        ?await awaitSpawn(sig.wait())
        check not task.ok.load()
      success()

    (await runTest()).tryGet()

  test "spawnJoin extracts generic result":
    proc runIntTask(ctx: SharedPtr[TaskCtx[int]]) {.gcsafe.} =
      ctx[].result = ThreadSpawnRes[int].ok(123)
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let value = ?await spawnJoin(runIntTask)
      check value == 123
      success()

    (await runTest()).tryGet()

  test "spawnJoin with void result":
    proc runVoidTask(ctx: SharedPtr[TaskCtx[void]]) {.gcsafe.} =
      ctx[].result = ThreadSpawnRes[void].ok()
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let res = await spawnJoin(runVoidTask)
      check res.isOk
      success()

    (await runTest()).tryGet()

  test "spawnJoin extracts error result":
    proc runErrTask(ctx: SharedPtr[TaskCtx[int]]) {.gcsafe.} =
      ctx[].result = ThreadSpawnRes[int].err("boom")
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let result = await spawnJoin(runErrTask)
      check result.isErr
      check result.error.msg == "boom"
      success()

    (await runTest()).tryGet()

  test "spawnJoin with seq result (move, not copy)":
    proc runSeqTask(ctx: SharedPtr[TaskCtx[seq[int]]]) {.gcsafe.} =
      ctx[].result = ThreadSpawnRes[seq[int]].ok(@[1, 2, 3, 4, 5])
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let value = ?await spawnJoin(runSeqTask)
      check value == @[1, 2, 3, 4, 5]
      success()

    (await runTest()).tryGet()

  test "spawnJoin moves an acyclic ref payload":
    # The networkpeer Message shape: refs nested in a seq. The Isolate
    # check cannot see through sequences, and the refs are {.acyclic.}
    # (never registered in ORC's per-thread cycle registry), so the
    # worker-created ref crosses safely under unique ownership: the
    # constructor's Isolate check proves the value graph is fresh.
    proc runAcycTask(ctx: SharedPtr[TaskCtx[seq[AcyclicBlock]]]) {.gcsafe.} =
      ctx[].result = ThreadSpawnRes[seq[AcyclicBlock]].ok(
        @[AcyclicBlock(cid: 7, data: @[1'u8, 2'u8, 3'u8])]
      )
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let value = ?await spawnJoin(runAcycTask)
      check value.len == 1
      check value[0].cid == 7
      check value[0].data == @[1'u8, 2'u8, 3'u8]
      success()

    (await runTest()).tryGet()

  test "spawnJoin with typed E and errMap":
    proc runTypedErrTask(ctx: SharedPtr[TaskCtx[int, TaskErr]]) {.gcsafe.} =
      ctx[].result =
        ThreadSpawnRes[int, TaskErr].err(TaskErr(code: 42, msg: "typed boom"))
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let result = await spawnJoin(
        runTypedErrTask,
        errMap = proc(e: TaskErr): ref CatchableError =
          newException(CatchableError, e.msg),
      )
      check result.isErr
      check result.error.msg == "typed boom"
      success()

    (await runTest()).tryGet()

  test "spawnJoin nil errMap falls back to SpawnFailure for non-string E":
    proc runTypedErrTask(ctx: SharedPtr[TaskCtx[int, TaskErr]]) {.gcsafe.} =
      ctx[].result =
        ThreadSpawnRes[int, TaskErr].err(TaskErr(code: 7, msg: "fallback boom"))
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let result = await spawnJoin(runTypedErrTask)
      check result.isErr
      check result.error of SpawnFailure
      check result.error.msg == "fallback boom"
      success()

    (await runTest()).tryGet()

  test "awaitSpawn cancellation drains worker before returning":
    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      withThreadSignal(sig):
        var task = ToyTask(value: 99, signal: sig)
        tp.spawn toySlowWorker(addr task)
        let spawnFut = awaitSpawn(sig.wait())

        # Give the worker time to start, then cancel
        await sleepAsync(1.millis)
        await spawnFut.cancelAndWait()

        # awaitSpawn should report cancellation
        check spawnFut.cancelled()

        # The noCancel drain must have waited for the worker to finish
        # before returning - task is still valid because the signal
        # close defer hasn't freed it yet.
        check task.ok.load()
      success()

    (await runTest()).tryGet()

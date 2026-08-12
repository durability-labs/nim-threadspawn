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

# Walker coverage: shapes the containsGCRef walker must classify correctly.
# Direct calls exercise the typeDesc-unwrap path (which used to return false
# for every named type).
type
  AcyclicBlock {.acyclic.} = ref object of RootObj
    cid: int
    data: seq[byte]

  AcyclicHolder = object
    blk: AcyclicBlock

  AcyclicWithRef {.acyclic.} = ref object of RootObj
    inner: ref int

  VariantWithRef = object
    case k: bool
    of true:
      r: ref int
    of false:
      i: int

  BaseWithRef = object of RootObj
    r: ref int

  ChildWithRef = object of BaseWithRef
    y: int

static:
  # forbidden: cycle-tracked refs, closures, and refs hidden in containers
  doAssert containsGCRef(ref int)
  doAssert containsGCRef(seq[ref int])
  doAssert containsGCRef(Option[ref int])
  doAssert containsGCRef(tuple[a: int, b: ref int])
  doAssert containsGCRef((int, ref int))
  doAssert containsGCRef(proc(x: int))
  doAssert containsGCRef(proc(x: int) {.closure.})
  # forbidden: variant branches and inherited base fields
  doAssert containsGCRef(VariantWithRef)
  doAssert containsGCRef(ChildWithRef)
  # forbidden: acyclic annotation does not excuse nested cycle-tracked refs
  doAssert containsGCRef(AcyclicWithRef)
  # allowed: values, raw pointers, function pointers
  doAssert not containsGCRef(int)
  doAssert not containsGCRef(seq[byte])
  doAssert not containsGCRef(ptr int)
  doAssert not containsGCRef(pointer)
  doAssert not containsGCRef(proc(x: int) {.nimcall.})
  # allowed: acyclic refs, including as object fields (field-name syms must
  # not be walked as types - regression guard)
  doAssert not containsGCRef(AcyclicBlock)
  doAssert not containsGCRef(AcyclicHolder)

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
      var r = ThreadSpawnRes[int].ok(123)
      ctx[].result = unsafeIsolate(move r)
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let value = ?await spawnJoin[int](runIntTask)
      check value == 123
      success()

    (await runTest()).tryGet()

  test "spawnJoin with void result":
    proc runVoidTask(ctx: SharedPtr[TaskCtx[void]]) {.gcsafe.} =
      var r = ThreadSpawnRes[void].ok()
      ctx[].result = unsafeIsolate(move r)
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let res = await spawnJoin[void](runVoidTask)
      check res.isOk
      success()

    (await runTest()).tryGet()

  test "spawnJoin extracts error result":
    proc runErrTask(ctx: SharedPtr[TaskCtx[int]]) {.gcsafe.} =
      var r = ThreadSpawnRes[int].err("boom")
      ctx[].result = unsafeIsolate(move r)
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let result = await spawnJoin[int](runErrTask)
      check result.isErr
      check result.error.msg == "boom"
      success()

    (await runTest()).tryGet()

  test "spawnJoin with seq result (move, not copy)":
    proc runSeqTask(ctx: SharedPtr[TaskCtx[seq[int]]]) {.gcsafe.} =
      var r = ThreadSpawnRes[seq[int]].ok(@[1, 2, 3, 4, 5])
      ctx[].result = unsafeIsolate(move r)
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let value = ?await spawnJoin[seq[int]](runSeqTask)
      check value == @[1, 2, 3, 4, 5]
      success()

    (await runTest()).tryGet()

  test "spawnJoin moves an acyclic ref payload":
    # {.acyclic.} refs are never entered in ORC's per-thread cycle registry,
    # so a worker-created ref crosses safely under unique ownership: the
    # worker writes the result and never touches the payload again.
    proc runAcycTask(ctx: SharedPtr[TaskCtx[AcyclicBlock]]) {.gcsafe.} =
      var r =
        ThreadSpawnRes[AcyclicBlock].ok(AcyclicBlock(cid: 7, data: @[1'u8, 2'u8, 3'u8]))
      ctx[].result = unsafeIsolate(move r)
      discard ctx[].signal.fireSync()

    proc runTest(): Future[?!void] {.async: (raises: [CancelledError]).} =
      let value = ?await spawnJoin[AcyclicBlock](runAcycTask)
      check value.cid == 7
      check value.data == @[1'u8, 2'u8, 3'u8]
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

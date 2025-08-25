# ez2.py  —  tiny, spawn-safe, asyncio + ProcessPool pipelines
from __future__ import annotations
import asyncio as aio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable, List, Optional, Sequence, Tuple, Union
import os, signal, sys, traceback

# -------------------------
# Public API
# -------------------------

Fn = Callable[..., Any]                 # sync fn
AFn = Callable[..., Awaitable[Any]]     # async fn

@dataclass
class StageSpec:
    fn: Union[Fn, AFn]                  # function to run (sync or async)
    workers: int = 1                    # parallelism for this stage
    preserve_order: bool = False        # preserve input order across this stage?
    executor: str = "process"           # "process" | "thread" | "inline"
    name: Optional[str] = None          # label for debugging

def Map(fn: Union[Fn, AFn], workers: int = 1, *, order: bool=False, executor="process", name=None):
    return StageSpec(fn, workers=workers, preserve_order=order, executor=executor, name=name or fn.__name__)

def Filter(pred: Union[Fn, AFn], workers: int = 1, *, order: bool=False, executor="process", name=None):
    # A filter is a map that returns None to drop
    async def _f(x):
        ok = await _maybe_await(pred, x)
        return x if ok else _DROP
    return StageSpec(_f, workers=workers, preserve_order=order, executor=executor, name=name or getattr(pred, "__name__", "filter"))

def Batch(size: int, *, order: bool=True, name=None):
    # Groups items into lists of length<=size (last batch may be smaller)
    async def _batcher(stream_in: aio.Queue, stream_out: aio.Queue, stop: aio.Event):
        buf = []
        while not (stop.is_set() and stream_in.empty()):
            try:
                item = await aio.wait_for(stream_in.get(), timeout=0.1)
            except aio.TimeoutError:
                item = _SENTINEL if stop.is_set() else _NOTHING
            if item is _NOTHING:
                continue
            if item is _SENTINEL:
                if buf:
                    await stream_out.put(buf)
                await stream_out.put(_SENTINEL)
                break
            buf.append(item)
            if len(buf) >= size:
                await stream_out.put(buf)
                buf = []
            stream_in.task_done()
    return _BatchSpec(_batcher, name or f"batch({size})")

# -------------------------
# Internals
# -------------------------

_SENTINEL = object()
_DROP = object()
_NOTHING = object()

class _BatchSpec:
    def __init__(self, runner, name): self.runner = runner; self.name = name

async def _maybe_await(fn, *args, **kw):
    res = fn(*args, **kw)
    if aio.iscoroutine(res):
        return await res
    return res

class Pipeline:
    """Build: Pipeline(stages...).run(iterable)"""
    def __init__(self, *stages: Sequence[Union[StageSpec, _BatchSpec]]):
        if not stages: raise ValueError("Pipeline needs at least one stage")
        self.stages: List[Union[StageSpec,_BatchSpec]] = list(stages)
        self._proc_pool: Optional[ProcessPoolExecutor] = None
        self._thread_pool: Optional[ThreadPoolExecutor] = None
        self._stop = aio.Event()
        self._tasks: List[aio.Task] = []
        self._queues: List[aio.Queue] = []

    async def __aenter__(self):
        self._proc_pool = ProcessPoolExecutor(max_workers=os.cpu_count() or 1)
        self._thread_pool = ThreadPoolExecutor(max_workers=64)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._stop.set()
        await self._graceful_cancel()
        if self._proc_pool: self._proc_pool.shutdown(cancel_futures=True)
        if self._thread_pool: self._thread_pool.shutdown(cancel_futures=True)

    async def run(self, source: Iterable[Any]) -> List[Any]:
        """Consume an iterable, return list of sink outputs."""
        async with self:
            loop = aio.get_running_loop()
            # queues between stages (bounded for backpressure)
            self._queues = [aio.Queue(maxsize=1024) for _ in range(len(self.stages)+1)]
            # feeder
            self._tasks.append(aio.create_task(self._feeder(source, self._queues[0])))
            # stage runners
            for i, spec in enumerate(self.stages):
                q_in, q_out = self._queues[i], self._queues[i+1]
                if isinstance(spec, _BatchSpec):
                    self._tasks.append(aio.create_task(spec.runner(q_in, q_out, self._stop)))
                else:
                    self._tasks.extend(await self._stage_tasks(spec, q_in, q_out))
            # sink gather
            outputs: List[Any] = []
            self._tasks.append(aio.create_task(self._sink_collector(self._queues[-1], outputs)))
            # signal handling
            _install_signal_handlers(self._stop)
            # run
            await aio.gather(*self._tasks, return_exceptions=False)
            return outputs

    async def _feeder(self, source, q: aio.Queue):
        try:
            for item in source:
                await q.put(item)
            await q.put(_SENTINEL)
        except BaseException:
            await q.put(_SENTINEL)
            raise

    async def _sink_collector(self, q: aio.Queue, out: List[Any]):
        while True:
            item = await q.get()
            if item is _SENTINEL:
                break
            out.append(item)
            q.task_done()

    async def _stage_tasks(self, spec: StageSpec, q_in: aio.Queue, q_out: aio.Queue) -> List[aio.Task]:
        tasks = []
        if spec.preserve_order:
            tasks.append(aio.create_task(self._ordered_stage(spec, q_in, q_out)))
        else:
            for _ in range(spec.workers):
                tasks.append(aio.create_task(self._unordered_stage(spec, q_in, q_out)))
        return tasks

    async def _unordered_stage(self, spec: StageSpec, q_in: aio.Queue, q_out: aio.Queue):
        loop = aio.get_running_loop()
        exec = self._pick_exec(spec.executor)
        try:
            while True:
                item = await q_in.get()
                if item is _SENTINEL:
                    # propagate one sentinel downstream when last worker observes it
                    await q_out.put(_SENTINEL)
                    break
                try:
                    res = await _submit(loop, exec, spec.fn, item)
                    if res is _DROP:
                        pass
                    elif _is_iterable_but_not_bytes(res):
                        for r in res: await q_out.put(r)
                    elif res is not None:
                        await q_out.put(res)
                except BaseException as e:
                    _print_exc(spec.name, e)
                    self._stop.set()
                    await q_out.put(_SENTINEL)
                    break
                finally:
                    q_in.task_done()
        except aio.CancelledError:
            pass

    async def _ordered_stage(self, spec: StageSpec, q_in: aio.Queue, q_out: aio.Queue):
        """Preserve order: submit tasks with sequence numbers; emit in order."""
        loop = aio.get_running_loop()
        exec = self._pick_exec(spec.executor)
        next_seq = 0
        pending: dict[int, aio.Task] = {}
        finished: dict[int, Any] = {}
        seq = 0
        try:
            while True:
                item = await q_in.get()
                if item is _SENTINEL:
                    # wait remaining
                    await _drain_ordered(pending, finished, next_seq, q_out)
                    await q_out.put(_SENTINEL)
                    break
                t = aio.create_task(_submit(loop, exec, spec.fn, item))
                pending[seq] = t
                # emit in order as soon as possible
                await _emit_ready(pending, finished, q_out, next_seq)
                if seq - next_seq > 1024:  # basic backpressure guard
                    await _emit_until(pending, finished, q_out, seq - 512, next_seq)
                seq += 1
                q_in.task_done()
        except aio.CancelledError:
            pass
        except BaseException as e:
            _print_exc(spec.name, e)
            self._stop.set()
            await q_out.put(_SENTINEL)

    def _pick_exec(self, name: str):
        if name == "inline": return None
        if name == "thread": return self._thread_pool
        return self._proc_pool

    async def _graceful_cancel(self):
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await aio.gather(*self._tasks, return_exceptions=True)

# -------------------------
# Helpers
# -------------------------

async def _submit(loop, exec, fn, item):
    if exec is None:
        return await _maybe_await(fn, item)
    # run sync/async fn in chosen executor
    return await loop.run_in_executor(exec, _call_sync, fn, item)

def _call_sync(fn, x):
    try:
        r = fn(x)
        return r
    except BaseException as e:
        raise e

def _is_iterable_but_not_bytes(x):
    return isinstance(x, (list, tuple)) and not isinstance(x, (bytes, bytearray))

async def _emit_ready(pending, finished, q_out, next_seq):
    # collect done tasks into finished
    done = [k for k,t in pending.items() if t.done()]
    for k in done:
        try:
            finished[k] = await pending[k]
        except BaseException as e:
            _print_exc("stage", e)
            finished[k] = _DROP
        del pending[k]
    # emit in-order
    while next_seq in finished:
        val = finished.pop(next_seq)
        if val is _DROP:
            pass
        elif _is_iterable_but_not_bytes(val):
            for v in val: await q_out.put(v)
        elif val is not None:
            await q_out.put(val)
        next_seq += 1
    return next_seq

async def _emit_until(pending, finished, q_out, target_seq, next_seq):
    while next_seq <= target_seq:
        next_seq = await _emit_ready(pending, finished, q_out, next_seq)
        await aio.sleep(0)

async def _drain_ordered(pending, finished, next_seq, q_out):
    # wait all then emit in order
    for t in list(pending.values()):
        try: await t
        except: pass
    await _emit_ready(pending, finished, q_out, next_seq)

def _install_signal_handlers(stop_event):
    def _handler(signum, frame):
        stop_event.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: signal.signal(sig, _handler)
        except Exception: pass

def _print_exc(stage, e):
    tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print(f"[{stage}] EXCEPTION\n{tb}", file=sys.stderr)

# -------------------------
# Example usage
# -------------------------
if __name__ == "__main__":
    import math, time

    def cpu_heavy(x):
        # pretend CPU work
        s = 0.0
        for i in range(20000):
            s += math.sin(x) * math.cos(i)
        return x, s

    async def main():
        pipe = Pipeline(
            Map(lambda x: x, workers=1, executor="inline", name="source"),
            Map(cpu_heavy, workers=os.cpu_count() or 4, order=True, executor="process", name="cpu"),
            Filter(lambda pair: pair[0] % 2 == 0, workers=1, executor="inline"),
            Batch(8),
            Map(lambda batch: sum(v for _, v in batch), workers=1, executor="inline", name="reduce"),
        )
        t0 = time.perf_counter()
        out = await pipe.run(range(0, 200))
        dt = time.perf_counter() - t0
        print(f"batches: {len(out)}   first: {out[0]:.3f}   time: {dt:.3f}s")

    aio.run(main())

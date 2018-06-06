"""
F.py -- An experimental library of python multiprocessing.
"""

from multiprocessing import Process, JoinableQueue, Event, Pipe
from multiprocessing import queues as MPQueues
from queue import Empty, PriorityQueue

import collections
import heapq
import time
import asyncio
import traceback
import itertools
import random
import os
from uuid import uuid4
import numpy as np

# A few fancy tricks...

from setproctitle import setproctitle

import ctypes

LIBC = ctypes.CDLL("libc.so.6")
sched_yield = LIBC.sched_yield

# def sched_yield():
#    pass

# def sched_yield():
#     time.sleep(0)
# def setproctitle():
#     pass


###################
# Process Instance #
####################


class F(Process):
    "I am a Process with a few useful hooks, latches, and queue management."

    # Aliases
    Event = Event
    Queue = JoinableQueue
    Pipe = Pipe

    # Primary Lifecycle
    # =================

    def initialize(self, *args, **kwargs):
        "Override me"

    def setup(self, *args, **kwargs):
        "Override me"

    def do(self, data):
        "Override me"
        self.put(data)

    def teardown(self):
        "Override me"

    # Helpers
    # =======

    def myAsync(self, coroutine):
        "Call and Excecute a async function"
        if not hasattr(self, "event_loop"):
            self.event_loop = asyncio.new_event_loop()
        return self.event_loop.run_until_complete(coroutine())

    def async(self, coroutine):
        "Execute a async function"
        if not hasattr(self, "event_loop"):
            self.event_loop = asyncio.new_event_loop()
        return self.event_loop.run_until_complete(coroutine)

    def async2(self, coroutine):
        "Execute a async function (when already in an event loop)"
        if not hasattr(self, "event_loop"):
            self.event_loop = asyncio.get_event_loop()
        pass

    def sleep(self, n=0.001):
        "Pause for a moment"
        time.sleep(n)

    def info(self):
        return [
            [q.qsize() for q in self.inputs],
            [q.qsize() for q in self.outputs],
            "{}".format(self.name),
        ]

    # Input / Output
    # ==============

    def furcator(self, data):
        meta = data[0]
        item = data[1]
        if isinstance(item, Data):
            item = item.furcate()
        return (meta, item)

    def put(self, item):
        "Emit some data"
        self.accounting_finish()

        data = (self.meta.copy(), item)

        first = True
        for output in self.outputs:
            if not isinstance(output, Indurate.ProxyQueue):
                if first:
                    output.put(data)
                    first = False
                else:
                    output.put(self.furcator(data))

        sched_yield()

    def get(self):
        "Recieve some data"

        if len(self.inputs) == 0:
            "Nothing to see here"
            return None

        data = None
        while data is None and not self.done():

            for i, q in enumerate(self.inputs):
                try:
                    data = q.get_nowait()
                except Empty:
                    continue
                else:
                    q.task_done()
                    break

            sched_yield()

        if data is None:
            self.meta = {}
            return None
        else:
            self.meta, item = data
            return item

    # Accounting
    # ==========

    def accounting_begin(self):
        self.accounting_start_time = time.time()

    def accounting_finish(self):
        if not "timings" in self.meta:
            self.meta["timings"] = {}
        if hasattr(self, "accounting_start_time"):
            self.meta["timings"][self.name] = time.time() - self.accounting_start_time

    # Lifecycle
    # =========

    def __init__(self, *args, **kwargs):
        "I am not yet a Process."
        "I have not started yet."

        super(Process, self).__init__()
        "Now I am an unstarted Process."

        self.env = kwargs.get("env", {})
        self.args = args
        self.kwargs = kwargs

        self.inputs = []
        self.outputs = []
        self.infrom = []

        self.is_source = False

        self.__done_event = self.Event()
        self.__stop_event = self.Event()
        self.__start_event = self.Event()

    # [Lifecycle] initialize(self, *args, **kwargs)

    def start(self):
        if not self.started():
            self.__start_event.set()
            self.__beforeStart()
            super(Process, self).start()
            self.__afterStart()
        return self

    def __beforeStart(self):
        self.parent_env = os.environ.copy()
        for k, v in self.env.items():
            os.environ[k] = v
        self.initialize(*self.args, **self.kwargs)

    def __afterStart(self):
        for k, v in self.env.items():
            if k in self.parent_env:
                os.environ[k] = self.parent_env[k]
            else:
                del os.environ[k]

    def run(self):
        setproctitle("python {}".format(self.name))
        self.meta = {}
        try:
            self.setup(*self.args, **self.kwargs)
        except Exception as e:
            self.handle_exception(e)

        while True:

            if self.stopped():

                return self.shutdown()
            else:
                try:
                    item = self.get()

                    if item is None:
                        pass
                    else:
                        self.accounting_begin()
                        try:
                            self.do(item)
                        except Exception as e:
                            self.handle_exception(e)

                except Exception as error:
                    traceback.print_exc()
                    return error

            if self.done():

                self.stop()

            sched_yield()

    def handle_exception(self, e):
        self.exception_handler.put((e, traceback.format_tb(e.__traceback__)))

    def started(self):
        return self.__start_event.is_set()

    # [Lifecycle] setup(self, *args, **kwargs)

    # [Lifecycle] do(self, item)

    def done(self):
        return not any([not fn.finished() for fn in self.infrom])

    def stop(self):

        self.__stop_event.set()
        return self

    def shutdown(self):
        try:
            self.teardown()
        except Exception as e:
            self.handle_exception(e)

        [q.join() for q in self.outputs]
        self.__done_event.set()

    # [Lifecycle] teardown(self)

    def stopped(self):
        return self.__stop_event.is_set()

    def finished(self):
        return self.__done_event.is_set()

    # Other helpers
    # =============

    def daemonize(self):
        self.daemon = True
        return self


# Main entry point


def EZ(*fns, **kwArgs):
    # just adds Sink() on the end.
    ind = Indurate(list(fns) + [Sink()], **kwArgs)
    # and set up the dependancy graph
    ind.brdep()
    return ind


class Indurate:

    def __init__(self, fns, qsize=16, inputs=None, outputs=None):

        self.qsize = qsize

        self.watchdog = None
        self.catch_added = False

        if callable(fns):
            name = fns.__name__
            fns = [Map(fns)]
            fns[0].name = "[{}] {}".format(name, fns[0].name)

        if len(fns) == 0:
            raise ValueError("process functions are required")

        self.inputs = [] if inputs is None else inputs
        self.outputs = [] if outputs is None else outputs

        self.fns = self.link(fns)
        self.deps = []

    def graph(self):

        g = self.mode()  # .link sets mode. todo: kill it

        for fn in self.fns:

            if isinstance(fn, Indurate):
                toAdd = fn.graph()
            else:
                toAdd = fn

            if isinstance(g, (list)):
                g.append(toAdd)
            elif isinstance(g, (set)):
                g.add(toAdd)
            elif isinstance(g, (tuple)):
                g = (*g, toAdd)
            else:
                raise ValueError("Graph Error, bad g type.")

        return g

    def brdep(self):
        """set .infrom"""
        snap = flatten(self.graph())
        for fA in snap:
            for fB in snap:
                if len(list(set(fA.outputs) & set(fB.inputs))) > 0:
                    fB.infrom.append(fA)
        for fn in flatten(self.graph()):
            if len(fn.infrom) == 0:
                fn.is_source = True

    def sinks(self):
        return [fn for fn in flatten(self.graph()) if isinstance(fn, Sink)]

    def sources(self):
        return [fn for fn in flatten(self.graph()) if fn.is_source]

    def xcut(self, attr, ind):
        # cross cut another induration into this one
        q = Indurate.ProxyQueue(self.qsize)
        for src in ind.sources():
            src.inputs.append(q)
        for sink in self.sinks():
            sink.outputs.append(q)
            for src in ind.sources():
                src.infrom.append(sink)
        for fn in flatten(self.graph()):
            setattr(fn, attr, q)
        self.deps.append(ind)
        return self

    def link(self, fns):

        self.mode = type(fns)

        if isinstance(fns, (tuple, Parallel)):
            return self.linkParallel(fns)
        elif isinstance(fns, (set, Broadcast)):
            return self.linkBroadcast(fns)
        elif isinstance(fns, (list, Serial)):
            return self.linkSerial(fns)
        else:
            raise ValueError("unknown link mode")

    def linkSerial(self, fns):
        # fns is a [list]
        #     containing runnable functions
        #             or Indurate instances
        #             or list, set, tuples, Serial, Parllel, or Broadcast

        for i in range(len(fns) - 1):

            fnA = fns[i]
            fnB = fns[i + 1]

            if not isinstance(fnA, (F, Indurate)):
                fnA = Indurate(fnA, self.qsize)
                fns[i] = fnA

            if not isinstance(fnB, (F, Indurate)):
                fnB = Indurate(fnB, self.qsize)
                fns[i + 1] = fnB

            if len(fnA.outputs) == 0 and len(fnB.inputs) == 0:
                q = JoinableQueue(self.qsize)
                fnA.outputs.append(q)
                fnB.inputs.append(q)

            else:
                applyunion(fnA.outputs, fnB.inputs)

        fns[0].inputs = self.inputs
        fns[-1].outputs = self.outputs

        return fns

    def linkParallel(self, fns):
        # fns will be a (tuple) or a Parallel(list)
        #     containing runnable functions
        #             or Indurate instances
        #             or list, set, tuples, Serial, Parllel, or Broadcast
        fns = list(fns)

        for i in range(len(fns)):

            if not isinstance(fns[i], (F, Indurate)):
                fns[i] = Indurate(fns[i], self.qsize, self.inputs, self.outputs)

            fns[i].inputs = self.inputs
            fns[i].outputs = self.outputs

        return fns

    def linkBroadcast(self, fns):
        # fns may be a {set} or a Broadcast(list)

        fns = list(fns)
        for i in range(len(fns)):

            if not isinstance(fns[i], (F, Indurate)):
                fns[i] = Indurate(fns[i], self.qsize, self.inputs, self.outputs)

            q = JoinableQueue(self.qsize)
            self.inputs.append(q)
            fns[i].inputs.append(q)
            fns[i].outputs = self.outputs

        return fns

    def info(self):
        return [fn.info() for fn in self.fns]

    def printLayout(self, d=0):
        for f in self.fns:
            if isinstance(f, Indurate):
                f.printLayout(d + 1)
            else:
                [id(o) for o in f.inputs]
                print(
                    ">>" + ">>" * d,
                    f.name,
                    [o.name for o in f.infrom],
                    str(id(f.inputs))[-5:],
                    str(id(f.outputs))[-5:],
                    [str(id(o))[-5:] for o in f.inputs],
                    [str(id(o))[-5:] for o in f.outputs],
                )

    def start(self):
        if not self.catch_added:
            self.catch()

        [dep.start() for dep in self.deps]
        [fn.start() for fn in self.fns]
        return self

    def watch(self, n=5.):
        [dep.watch() for dep in self.deps]
        self.watchdog = Indurate.WatchDog(self, n)
        self.watchdog.start()
        return self

    def unwatch(self):
        [dep.unwatch() for dep in self.deps]
        self.watchdog.stop()
        self.watchdog.join()
        self.watchdog = None

    def daemonize(self):
        [dep.daemonize() for dep in self.deps]
        [fn.daemonize() for fn in self.fns]
        return self

    def set_catch_added(self):
        self.catch_added = True
        [dep.set_catch_added() for dep in self.deps]

    def catch(self, handler=None):

        if not self.catch_added:
            exception_queue = F.Queue()

            class ChildHandler(F):

                def do(self, exception):
                    exception_queue.put(exception)

            self.xcut("exception_handler", EZ(ChildHandler()))

            import threading

            def parentHandler(this):
                while not this.stopped():
                    try:
                        e, tb = exception_queue.get(True, 1)
                    except Empty:
                        continue
                    else:
                        this.stop()
                        if handler:
                            handler(e, tb)
                        else:

                            print(
                                "".join(
                                    traceback.format_exception(
                                        etype=type(e), value=e, tb=e.__traceback__
                                    )
                                    + tb
                                )
                            )

            self.exception_thread = threading.Thread(target=parentHandler, args=[self])
            self.exception_thread.start()
            self.set_catch_added()

        return self

    def join(self):
        self.start()
        [fn.join() for fn in self.fns]
        [dep.join() for dep in self.deps]

        if self.watchdog is not None:
            self.watchdog.stop()

        return self

    def stop(self):
        [fn.stop() for fn in self.fns]
        [dep.stop() for dep in self.deps]
        if self.watchdog is not None:
            self.watchdog.stop()
        return self

    def stopped(self):
        return not any([not fn.stopped() for fn in self.fns])

    def items(self):
        # get items from workflow

        sink_queues = flatten([sink.inputs for sink in self.sinks()])
        for sink in self.sinks():
            sink.inputs = []

        self.start()

        while not self.stopped():
            for q in sink_queues:
                try:
                    (meta, item) = q.get(timeout=0.1)
                    q.task_done()
                    yield item
                except:
                    pass

        while sum(q.qsize() for q in sink_queues) > 0:
            for q in sink_queues:
                try:
                    (meta, item) = q.get(False)
                    q.task_done()
                    yield item
                except:
                    sched_yield()

        return

    def list(self):
        return list(self.items())

    class WatchDog(F):

        def setup(self, link, n=10.):
            import pprint

            while not self.stopped():
                self.sleep(n / 2.)
                print(
                    pprint.pformat(
                        link.info(), indent=4, width=80, depth=10, compact=False
                    )
                )
                self.sleep(n / 2.)

    class ProxyQueue:

        def __init__(self, qsize=16):
            self.q = F.Queue(qsize)

        def get(self, *args, **kwArgs):
            return {}, self.q.get(*args, **kwArgs)

        def get_nowait(self, *args, **kwArgs):
            return {}, self.q.get_nowait(*args, **kwArgs)

        def put(self, item, *args, **kwArgs):
            return self.q.put(item, *args, **kwArgs)

        def put_nowait(self, *args, **kwArgs):
            return self.q.put_nowait(item, *args, **kwArgs)

        def qsize(self):
            return self.q.qsize()

        def empty(self):
            return self.q.empty()

        def full(self):
            return self.q.full()

        def close(self):
            return self.q.close()

        def join(self):
            return self.q.join()

        def task_done(self):
            return self.q.task_done()

        def join_thread(self):
            return self.q.join_thread()

        def cancel_join_thread(self):
            return self.q.cancel_join_thread()


class Serial(list):

    def __init__(self, *fns):
        super(Serial, self).__init__(fns)


S = Serial


class Parallel(list):

    def __init__(self, *fns):
        super(Parallel, self).__init__(fns)


P = Parallel


class Broadcast(list):

    def __init__(self, *fns):
        super(Broadcast, self).__init__(fns)


B = Broadcast


def applyunion(a, b):
    comb = list(set([*a, *b]))
    [l.clear() for l in [a, b]]
    a.extend(comb)
    b.extend(comb)
    return comb


def flatten(g):
    """Given a list, possibly nested to any level, return it flattened."""
    # modified from http://code.activestate.com/recipes/578948-flattening-an-arbitrarily-nested-list-in-python/
    flattened = []
    for o in g:
        if isinstance(o, (list, set, tuple, collections.Iterable)) and not isinstance(
            o, (str, bytes)
        ):
            flattened.extend(flatten(o))
        else:
            flattened.append(o)
    return flattened


# Parrallism wrappers
# ===================


def As(n, fn, *args, **kwargs):
    "Shortcut to parallize a function"
    return tuple(fn(*args, **kwargs) for _ in range(n))


def By(n, fn, *args, **kwargs):
    "Shortcut to broadcast a function"
    return {fn(*args, **kwargs) for _ in range(n)}


# A few core utility functions
# ============================


class Sink(F):

    def do(self, item):
        if len(self.outputs) > 0:
            self.put(item)


class Map(F):
    "Maps input -> output through function"

    def setup(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def do(self, item):
        self.put(self.fn(item, *self.args, **self.kwargs))


def Seq(*fns):
    "Ensures input ordering matches output ordering"
    key = str(random.random())
    start = SequenceStart(key)
    end = SequenceEnd(key)
    return [start, *fns, end]


class SequenceStart(F):
    "Adds a sequence id to meta"

    def setup(self, key="seq"):
        self.key = key
        self.seq_id = 0

    def do(self, item):
        self.meta[self.key] = self.seq_id
        self.put(item)
        self.seq_id += 1


class SequenceEnd(F):
    "Orders input based on sequence id"

    def setup(self, key="seq"):
        self.pq = PriorityQueue()
        self.key = key
        self.seq_id = 0

    def do(self, item):
        self.pq.put((self.meta[self.key], (self.meta, item)))

        for _ in range(self.pq.qsize()):
            p, data = self.pq.get()
            if p == self.seq_id:
                self.meta, item = data
                del self.meta[self.key]
                self.put(item)
                self.seq_id += 1
            else:
                self.pq.put((p, data))
                break


# Large data package sending mechanism
# ====================================


class Data:

    def save_gen_fname(self, key):
        return "/tmp/mpyx_datagram_{}_{}.npy".format(key, str(uuid4()))

    def save(self, key, data):

        if not hasattr(self, "np_file_store"):
            # potential todo: https://stackoverflow.com/questions/394770/override-a-method-at-instance-level?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
            # rewrite this instance's save function to skip this check in the future.
            self.np_file_store = {}

        if key in self.np_file_store:
            fname = self.np_file_store[key]
        else:
            fname = self.save_gen_fname(key)
        np.save(fname, data)
        self.np_file_store[key] = fname

    def load(self, key):
        try:
            return np.load(self.np_file_store[key], "r")
        except ValueError as e:
            return np.load(self.np_file_store[key])

    def furcate(self):
        clone = pickle.loads(pickle.dumps(self))
        if hasattr(clone, "np_file_store"):
            for key, fname in clone.np_file_store.items():
                new_fname = self.save_gen_fname(key)
                os.link(fname, new_fname)
                clone.np_file_store[key] = new_fname
        return clone

    def clean(self):
        if hasattr(self, "np_file_store"):
            for key, fname in self.np_file_store.items():
                os.remove(fname)

    def erase(self, prop):
        if hasattr(self, "np_file_store"):
            fname = self.np_file_store.pop(prop)
            os.remove(fname)

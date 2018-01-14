"""
F.py -- An experimental library of python multiprocessing.
"""

from multiprocessing import Process, JoinableQueue, Event
from queue import Empty, PriorityQueue

import collections
import heapq
import time
import asyncio
import traceback
import itertools
import random
import os


# A few fancy tricks...

from setproctitle import setproctitle
import ctypes
LIBC = ctypes.CDLL('libc.so.6')
sched_yield = LIBC.sched_yield

# def sched_yield():
#     time.sleep(0)
# def setproctitle():
#     pass


###################
# Process Instance #
####################


class F(Process):
    "I am a Process with a few useful hooks, latches, and queue management."
    
    
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
    

    def async(self, coroutine):
        "Call a async function"
        if not hasattr(self, "event_loop"):
            self.event_loop = asyncio.new_event_loop()
        return self.event_loop.run_until_complete(coroutine)
    
    def sleep(self, n=0.001):
        "Pause for a moment"
        time.sleep(n)
    
    
    def info(self):
        return [
            [q.qsize() for q in self.inputs],
            [q.qsize() for q in self.outputs],
            "{}".format(self.name)
        ]
    
    
    # Input / Output
    # ==============
    
    def put(self, item):
        "Emit some data"
        data = (self.meta.copy(), item)
        [output.put(data) for output in self.outputs]
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
            
    
    # Lifecycle
    # =========
    
    def __init__(self, *args, **kwargs):
        "I am not yet a Process."
        "I have not started yet."
        
        super(Process, self).__init__()
        "Now I am an unstarted Process."
        
        self.env = kwargs.get('env', {})
        self.args = args
        self.kwargs = kwargs
        
        self.inputs = []
        self.outputs = []
        self.infrom = [] 
        
        self.__done_event = Event()
        self.__stop_event = Event()
        self.__start_event = Event()
    
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
        self.setup(*self.args, **self.kwargs)
        while True:
            
            if self.stopped():
            
                return self.shutdown()
            else:
                try:
                    item = self.get()
            
                    if item is None:
                        pass
                    else:
                        self.do(item)
                        
                except Exception as error:
                    traceback.print_exc()
                    return error
                    
            if self.done():
            
                self.stop()
                
            sched_yield()
    
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
        self.teardown()
        [q.join() for q in self.outputs]
        self.__done_event.set()
    
    # [Lifecycle] teardown(self)
    
    def stopped(self):
        return self.__stop_event.is_set()
    
    def finished(self):
        return self.__done_event.is_set()
    




# Main entry point

def EZ(*fns, **kwArgs):
    # just adds Sink() on the end.
    ind = Indurate(list(fns) + [Sink()], **kwArgs)
    # and set up the dependancy graph
    ind.brdep()
    return ind




class Indurate:
    
    def __init__(self, fns, qsize = 16, inputs = None, outputs = None):
        

        self.qsize = qsize
        self.watchdog = None
        
        if callable(fns):
            name = fns.__name__
            fns = [Map(fns)]
            fns[0].name = "[{}] {}".format(name, fns[0].name)
        
        if len(fns) == 0:
            raise ValueError("process functions are required")
        
        self.inputs = [] if inputs is None else inputs
        self.outputs = [] if outputs is None else outputs
        
        self.fns = self.link(fns)
        
        
    
    
    def graph(self):
        
        g = self.mode() #.link sets mode. todo: kill it
        
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
            
    def wire(self, g, incoming = None):
        # incoming is the parent,
        # potentially deprecated in favor of self.brdep
        '''
        
        def wire(self, g, inc): ...   
        
        if isinstance(g, (list, Serial)): # serial
            # I don't know how to express this algo either.
        
            if g[0] is Fn
                g[0].inc = inc
                wire(g[1:], g[0])
            else
                wire(g[0], inc)
        
        if isinstance(g, (set, tuple, Parallel, Broadcast)):
            for all i in g
                if i is Fn
                    i.inc = inc
                else
                    wire(g[0], inc)
        '''
        incoming = [] if incoming is None else incoming
        
        kind = type(g)
        
        if len(g) == 0:
            return kind
        else:
            g = list(g) # this works because the linker does the i/o,
                        # wire just creates the dependancy graph
        
        fn = g[0]
        print("new call", 
        fn, g, incoming)
        
        if isinstance(fn, (list, set, tuple)):
            k = self.wire(fn, incoming)
            print("datastructure", k)
            if k in [set, tuple, Parallel, Broadcast]:
                self.wire(g[1:], fn)
            else: # Serial
                self.wire(g[1:], [fn[-1]])
                
            
        else:
            print("FInstance", kind, g)
            for e in self.endpoints(incoming):
                if e not in fn.infrom:
                    print("im", fn, "parent", e)
                    fn.infrom.append(e)
            
            if isinstance(g, (list, set, tuple)):
                print("G is list, set, tuple")
                self.wire(g[1:], [fn])
            else:
                print("G is not")
                self.wire(g[1:], incoming)
        return kind
    
    def endpoints(self, g):
        ends = []
        if len(g) > 0:
            g = list(g) # this works because the linker does the i/o,
                        # wire just creates the dependancy graph
            
            fn = g[0]
            if isinstance(fn, (list, set, tuple)):
                ends.extend(self.endpoints(fn[1:]))
                ends.extend(self.endpoints(g[1:]))
            else:
                ends.append(fn)
                ends.extend(self.endpoints(g[1:]))

        
        return ends
    
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
            fnB = fns[i+1]
            
            if not isinstance(fnA, (F, Indurate)):
                fnA = Indurate(fnA, self.qsize)
                fns[i] = fnA
                
            if not isinstance(fnB, (F, Indurate)):
                fnB = Indurate(fnB, self.qsize)
                fns[i+1] = fnB
            
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


    def printLayout(self, d = 0):
        for f in self.fns:
            if isinstance(f, Indurate):
                f.printLayout(d + 1)
            else:
                [id(o) for o in f.inputs]
                print(">>" + ">>" * d, f.name, 
                    [o.name for o in f.infrom], 
                    str(id(f.inputs))[-5:],
                    str(id(f.outputs))[-5:],
                    [str(id(o))[-5:] for o in f.inputs], 
                    [str(id(o))[-5:] for o in f.outputs])


    def start(self):
        [fn.start() for fn in self.fns]
        return self
    
    def watch(self, n = 5.):
        self.watchdog = Indurate.WatchDog(self, n)
        self.watchdog.start()
        return self
    
    def join(self):
        [fn.join() for fn in self.fns]
        
        if self.watchdog is not None:
            self.watchdog.stop()
        return self
    
    def stop(self):
        [fn.stop() for fn in self.fns]
        if self.watchdog is not None:
            self.watchdog.stop()
        return self
        
    def stopped(self):
        return not any([not fn.stopped() for fn in self.fns])
    
    def items(self):
        # get items from workflow
        
        sink_queues = self.fns[-1].inputs
        self.fns[-1].inputs = []
        del self.fns[-1]
        
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
        def setup(self, link, n = 5.):
            import pprint
            while not self.stopped():
                self.sleep(n/2.)
                print(pprint.pformat(link.info(), indent=4, width=80, depth=10, compact=False))
                self.sleep(n/2.)
                
                
                
                
        
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
        if isinstance(o, (list, set, tuple, collections.Iterable)) and not isinstance(o, (str, bytes)):
            flattened.extend(flatten(o))
        else:
            flattened.append(o)
    return flattened
    
"""
def flatten(g):
    ""https://stackoverflow.com/a/2158532/5357876""
    for o in g:
        if isinstance(o, collections.Iterable) and not isinstance(o, (str, bytes)):
            yield from flatten(o)
        else:
            yield o
"""

def As(n, fn, *args, **kwargs):
    "Shortcut to parallize a function"
    return tuple(fn(*args, **kwargs) for _ in range(n))

def By(n, fn, *args, **kwargs):
    "Shortcut to broadcast a function"
    return {fn(*args, **kwargs) for _ in range(n)}


class Sink(F):
    def do(self, item):
        pass


class Const(F):
    "Constant value generator"
    def setup(self, item, limit = None):
        count = 0
        while count < (limit or float('inf')):
            self.put(item)
            count += 1
        self.stop()


class Iter(F):
    "A simple iterator that flows an input iterable into the process graph"
    def setup(self, iterable):
        for x in iterable:
            self.put(x)
        self.stop()
    


class Print(F):
    "A simple iterator that flows an input iterable into the process graph"
    def setup(self, pre = None):
        self.pre = pre
    def do(self, item):
        print(item) if self.pre is None else print(self.pre, item)
        self.put(item)


class Stamp(F):
    "A simple debug tol to track items in the workflow"
    def setup(self, pre = "Processing item" ):
        self.count = 0
    def do(self, item):
        self.count += 1
        print(self.count) if self.pre is None else print(self.pre, self.count)
        self.put(item)



class Map(F):
    "Maps input -> output through function"
    def setup(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
    def do(self, item):
        self.put(self.fn(item, *self.args, **self.kwargs))


class Filter(F):
    "Filters input -> output by function"
    def setup(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
    def do(self, item):
        if self.fn(item, *self.args, **self.kwargs):
            self.put(item)


class Batch(F):
    "Groups input into batches of 'size'"
    def setup(self, size = 64):
        self.batch = []
        self.size = size
        
    def do(self, item):
        self.batch.append(item)
        if len(self.batch) >= self.size:
            self.put(self.batch)
            self.batch = []
    
    def teardown(self):
        if len(self.batch) > 0:
            self.put(self.batch)



def Seq(*fns):
    key = str(random.random())
    start = SequenceStart(key)
    end = SequenceEnd(key)
    return [start, *fns, end]


def Zip(*fns):
    if len(fns) == 1:
        fns = fns[0]
    key = str(random.random())
    start = SequenceStart(key)
    merge = SequenceMerge(key, len(fns))
    
    return [start, set(fns), merge]


class SequenceStart(F):
    "Adds a sequence id to meta"
    def setup(self, key = "seq"):
        self.key = key
        self.seq_id = 0
        
    def do(self, item):
        self.meta[self.key] = self.seq_id
        self.put(item)
        self.seq_id += 1


class SequenceEnd(F):
    "Orders input based on sequence id"
    def setup(self, key = "seq"):
        self.pq = PriorityQueue() 
        self.key = key
        self.seq_id = 0
        
    def do(self, item):
        self.pq.put((self.meta[self.key], (self.meta, item)))
        for _ in range(self.pq.qsize()): # todo: optimize
            p, data = self.pq.get()
            
            if p == self.seq_id:
                self.meta[self.key] = self.seq_id
                self.meta, item = data
                self.put(item)
                self.seq_id = self.seq_id + 1
            else:
                self.pq.put((p, data))


class SequenceMerge(F):
    "Merges inputs into rows by sequence id"
    def setup(self, key = "seq", n = None):
        self.pq = PriorityQueue() 
        self.key = key
        self.seq_id = 0
        self.n = len(set(self._infrom)) if n is None else n
        
    def do(self, item):
        
        self.pq.put((self.meta[self.key], item))
        
        if self.pq.qsize() >= self.n:
            for _ in range(self.pq.qsize()): # todo: optimize
                if self.pq.qsize() >= self.n:
                    keep = []
                    back = []
                    for i in range(self.n):
                        p, item = self.pq.get()
                        
                        if p == self.seq_id:
                            keep.append(item)
                        else:
                            back.append((p, item))
                    
                    if len(keep) == self.n:
                        self.meta[self.key] = self.seq_id
                        self.put(keep)
                        self.seq_id += 1
                    else:
                        for item in keep:
                            self.pq.put((self.seq_id, item))
                            
                    for data in back:
                        self.pq.put(data)



class Read(F):
    def setup(self, filename, mode='r'):
        with open(filename, mode) as file:
            for data in file:
                self.put(data)
        self.stop()


class Write(F):
    def setup(self, filename, mode='w'):
        self.file = open(filename, mode)
    
    def do(item):
        self.file.write(item)
        self.put(item)
    
    def teardown():
        self.file.close()



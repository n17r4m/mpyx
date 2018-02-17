"""
Process.py -- An experimental library of python multiprocessing.
### deprecated in favor of F.py ###
"""
from queue import Empty, PriorityQueue
import heapq
import multiprocessing as mp
import time
import asyncio
import traceback
import itertools
import random
import os

from setproctitle import setproctitle

"""

Synopsis
========

Examples
========

    
>>> import numpy as np
>>> from mpyx.Process import F, Print
>>> 
>>> class SumAndAdd(F):
>>>     def setup(self, x):
>>>         self.x = x
>>>         self.sum = 0
>>>     def do(self, n):
>>>         self.sum += n
>>>         self.push(self.x + n)
>>>     def teardown(self):
>>>         self.push([self.sum])
>>>
>>> Iter([1,2,3]).into(PointlessAddition(-1)).into(Print()).execute()
0
1
2
[6]


"""


####################
# Process Instance #
####################


class Zueue(mp.Process):
    """
    A few useful hooks, latches, and queues.
    I am a Process.
    """
    
    Event = mp.Event
    Queue = mp.Queue
    
    # Override me
    def initialize(self, *args, **kwargs):
        pass
    
    # Override me
    def setup(self, *args, **kwargs):
        pass
    
    # Override me
    def do(self, data):
        self.push(data)
    
    # Override me
    def teardown(self): 
        pass
    
    
    # Input / Output
    # ==============
    
    def push(self, item):
        
        data = None if item is None else (self.meta.copy(), item)
        self.queues_out.put(data)
    
    def pull(self):
        
        data = self.queues_in.get()
        
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
        super(Zueue, self).__init__()
        "I am now an unstarted Process."
        
        self.queues_in = IncomingQueueList()
        self.queues_out = OutgoingQueueList()
        
        self.stop_event = mp.Event()
        self.start_event = mp.Event()
        self._into = []
        self._infrom = []
        self.queue_out_idx = 0
        
        self.args = args
        self.kwargs = kwargs
        if not 'env' in self.kwargs:
            self.env = {}
        else:
            self.env = self.kwargs["env"]
            del self.kwargs["env"]
    
    # [Lifecycle] initialize(self, *args, **kwargs)
    
    def execute(self):
        self.start()
        [ep.join() for ep in self.endpoints()]
    
    def start(self):
        if not self.started():
            self.start_event.set()
            self.beforeStart()
            [infrom.start() for infrom in self._infrom]
            [into.start() for into in self._into]
            super(Zueue, self).start()
            self.afterStart()
        return self
    
    def beforeStart(self):
        self.parent_env = os.environ.copy()
        for k, v in self.env.items():
            os.environ[k] = v
        self.initialize(*self.args, **self.kwargs)
    
    def afterStart(self):
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
                    item = self.pull()
                    if item is None:
                        self.stop()
                    else:
                        self.do(item)
                except Exception as e:
                    traceback.print_exc()
                    return e
            self.sleep()
    
    def started(self):
        return self.start_event.is_set()
    
    # [Lifecycle] setup(self, *args, **kwargs)
    
    # [Lifecycle] do(self, item)
        
    def stop(self):
        self.stop_event.set()
        return self
        
    # [Lifecycle] teardown(self)
    
    def shutdown(self):
        [self.push(None) for i in range(len(self._into))]
        return self.teardown()

    def stopped(self):
        return self.stop_event.is_set()
    
    
    
    # Process Flow  (Must be used before start)
    # ============
    
    def into(self, zueues):
        zueues = ZueueList(listify(zueues))
        self.ensureOutputQueue()
        added = []
        
        for z in zueues:
            if not z in self._into:
                self._into.append(z)
                z._infrom.append(self)
                z.queues_in.extend(self.queues_out)
                added.append(z)
                
        return ZueueList(added)
    
    def infrom(self, zueues):
        zueues = ZueueList(listify(zueues))
        self._infrom.extend(zueues)
        for z in zueues:
            z.ensureOutputQueue()
            if not self in z._into:
                z._into.append(self)
            self.queues_in.extend(z.queues_out)
        return self
    
    
    def items(self, output_q = None):
        out = self.output() if output_q is None else output_q
        self.start()
        while True:
            
            item = out.pull()
            
            if item is None:
                break
            else:
                yield item
            self.sleep()
    
    # Aux Process Flow  (Must be used before start)
    # ================
    

    def input(self):
        return self.InputHandle(self.input_queue())
    
    def input_queue(self, queue = None):
        queue = self.Queue(16) if queue is None else queue
        self.queues_in.append(queue)
        return queue
    
    def output(self):
        q = self.output_queue()
        o_spaghetti = self.teardown
        def output_teardown():
            o_spaghetti()
            q.put(None)
        self.teardown = output_teardown
        return self.OutputHandle(q)
    
    def output_queue(self, queue = None):
        queue = self.Queue(16) if queue is None else queue
        self.queues_out.append(queue, "broadcasted")
        return queue
        
    
    
    # Shortcuts
    # =========
    
    def print(self, pre = None, post = None):
        return self.into(Print(pre, post))
    
    def stamp(self, pre = None):
        return self.into(Stamp(pre))
    
    def watchdog(self, n = 5., also_show = []):
        return self.into(Watchdog(n, also_show))
    
    def sequence(self, key = "seq_id"):
        return self.into(StartSeq(key))
    
    def order(self, key = "seq_id"):
        return self.into(EndSeq(key))
    
    def split(self, zueues):
        
        zueues = ZueueList(listify(zueues))
        
        for z in zueues.entrypoints():
            q = mp.Queue(16)
            self._into.append(z)
            self.queues_out.append(q, "broadcasted")
            z._infrom.append(self)
            z.queues_in.append(q)
        return zueues
    
    def merge(self, key = "seq_id", n = None):
        n = len(set(self._infrom)) if n is None else n
        return self.into(MergeSeq(key, n))
    
    def batch(self, size = 64):
        return self.into(Batch(size))
    
    def map(self, fn, *args, **kwargs):
        return self.into(Map(fn, *args, **kwargs))
    
    def filter(self, fn, *args, **kwargs):
        return self.into(Filter(fn, *args, **kwargs))
    
    # Helpers
    # =======
    
    def entrypoints(self):
        if len(self._infrom) > 0:
            return list(set(flatten([node.entrypoints() for node in self._infrom])))
        else:
            return [self]
    
    def endpoints(self):
        if len(self._into) > 0:
            return list(set(flatten([node.endpoints() for node in self._into])))
        else:
            return [self]
    
    def sleep(self, wait = 0.01):
        return time.sleep(wait)
    
    def ensureOutputQueue(self):
        if len(self.queues_out) == 0:
            self.queues_out.append(self.Queue(16))
    
    class InputHandle():
        def __init__(self, queue):
            self.queue = queue
        def push(self, item):
            if item is None:
                self.queue.put(None)
            else:
                self.queue.put(({}, item))
    
    class OutputHandle():
        def __init__(self, queue):
            self.queue = queue
        def pull(self):
            data = self.queue.get()
            if data is None:
                return None
            else:
                meta, item = data
                return item
    

######################
# Process Collection #
######################

class ZueueList(list):
    "A collection of Process"
    
    # Lifecycle
    
    def execute(self):
        [z.execute() for z in self]
        return self

    def start(self):
        [z.start() for z in self]
        return self
    
    # Process Flow

    def into(self, zueues): 
        return ZueueList(set(flatten([z.into(zueues) for z in self])))
        
    def infrom(self, zueues): 
        return ZueueList(set(flatten([z.infrom(zueues) for z in self])))

    # Shortcuts

    def items(self):
        qs = [z.output() for z in self]
        return merge_iterators([z[0].items(z[1]) for z in zip(self, qs)])

    def print(self, pre = None, post = None):
        return self.into(Print(pre, post))

    def stamp(self, pre = None):
        return self.into(Stamp(pre))
        
    def watchdog(self, n = 5., also_show = []):
        return self.into(Watchdog(n, also_show))

    def sequence(self, key = "seq_id"):
        return self.into(StartSeq(key))
    
    def order(self, key = "seq_id"):
        return self.into(EndSeq(key))
    
    def split(self, zueues): 
        return ZueueList(flatten([z.split(zueues) for z in self]))
    
    def merge(self, key = "seq_id", n = None):
        return self.into(MergeSeq(key, n))
    
    def batch(self, size = 64):
        return self.into(Batch(size))
    
    def map(self, fn, *args, **kwargs):
        return self.into(Map(fn, *args, **kwargs))
    
    def filter(self, fn, *args, **kwargs):
        return self.into(Filter(fn, *args, **kwargs))

    # Helpers
    
    def entrypoints(self):
        return list(set(flatten([z.entrypoints() for z in self])))
        
    def endpoints(self):
        return list(set(flatten([z.endpoints() for z in self])))
    


##################
# Helper Classes #
##################


class IncomingQueueList():
    def __init__(self):
        self.queues = []
    
    def __len__(self):
        return len(self.queues)
    
    def qsize(self):
        return sum([q.qsize() for q in self.queues])
    
    def append(self, q):
        self.queues.append(q)
        
    def extend(self, qs):
        
        if isinstance(qs, list):
            self.queues.extend(qs)
        elif isinstance(qs, IncomingQueueList):
            self.queues.extend(qs.queues)
        elif isinstance(qs, OutgoingQueueList):
            self.queues.extend(qs.queues["distributed"])
            self.queues.extend(qs.queues["broadcasted"])
        else:
            raise Exception("Unknown list type when extending IncomingQueueList")
        
    def get(self):
        if len(self.queues) == 0:
            "/dev/zero"
            return None
        
        data = False
        while data is False and len(self.queues) > 0:
            "This is a rickety engine."
            
            rm_list = [] # Queues evaporate when they push None.
            random.shuffle(self.queues) # There is no assumed order
            data = False # False is not None
            
            for i, qi in enumerate(self.queues):

                try: 
                    data = qi.get_nowait()
                except Empty:
                    continue
                
                if data is None:
                    rm_list.append(i)
                    data = False
                else:
                    break
            
            for index in sorted(rm_list, reverse=True):
                del self.queues[index]
            
            time.sleep(0.001)

        if data is False:
            return None
        else:
            return data
        
class OutgoingQueueList():
    def __init__(self):
        self.queues = {
            "distributed": [],
            "broadcasted": []
        }
        self.current_idx = 0
    
    def __len__(self):
        return sum([len(qs) for qs in self.queues.values()])
    
    def qsize(self):
        return sum([sum([q.qsize() for q in qs]) for qs in self.queues.values()])
    
    def append(self, q = None, kind = "distributed"):
        q = mp.Queue(16) if q is None else q
        self.queues[kind].append(q)

    def extend(self, qs = [], kind = "distributed"):
        if isinstance(qs, list):
            self.queues[kind].extend(qs)
        elif isinstance(qs, IncomingQueueList):
            self.queues["broadcasted"].append(qs) # send to all nested QueueLists
        elif isinstance(qs, OutgoingQueueList):
            self.queues["distributed"].extend(qs.queues["distributed"])
            self.queues["broadcasted"].extend(qs.queues["broadcasted"])
        else:
            raise Exception("Unknown list type when extending OutgoingQueueList")
        
    
    def put(self, data = None):
        
        if len(self.queues["distributed"]) > 0:
            self.queues["distributed"][self.current_idx].put(data)
            self.current_idx = (1 + self.current_idx) % len(self.queues["distributed"])
        
        for queue_out in self.queues["broadcasted"]:
            queue_out.put(data)



#################
# Process Nodes #
################# 

class F(Zueue):
    "Experimental async function call support"
    def async(self, coroutine):
        if not hasattr(self, "event_loop"):
            self.event_loop = asyncio.new_event_loop()
        return self.event_loop.run_until_complete(coroutine)
        

def By(n, f, *args, **kwargs):
    "Shortcut to parallize a function"
    return ZueueList([f(*args, **kwargs) for _ in range(n)])


class Iter(F):
    "A simple iterator that flows an input iterable into the process graph"
    def setup(self, iterable):
        for x in iterable:
            self.put(x)
        self.stop()


def Const(val):
    "Simply return the same value whenever pulled from"
    return Iter(itertools.repeat(val))


class Print(F):
    "Prints the item, then passes it along for further processing"
    def setup(self, pre = "", post = ""):
        self.pre, self.post, = pre, post
        
    def do(self, item):
        if self.pre and self.post:
            print(self.pre, item, self.post)
        elif self.pre:
            print(self.pre, item)
        elif self.post:
            print(item, self.post)
        else:
            print(item)
        self.push(item)


class Stamp(F):
    "A simple counter for debugging"
    def setup(self, pre = None, verbose=False):
        self.pre = pre
        self.count = 0
        self.verbose = verbose
    def do(self, item):
        self.count += 1
        if self.verbose:
            if self.pre:
                print(self.pre, self.count, item)
            else:
                print(self.count, item)
        else:
            if self.pre:
                print(self.pre, self.count)
            else:
                print(self.count)
        self.push(item)


class Watchdog(F):
    "Observes queue sizes every n seconds"
    def setup(self, n = 5., also_show = []):
        
        self.dog = self.Dog(n, self.proc_queue_list(also_show)).start()
    
    def teardown(self):
        self.dog.stop()
    
    def all_procs(self):
        def walk(ob, target):
            procs = set()
            for p in getattr(ob, target):
                procs.add(p)
                [procs.add(z) for z in walk(p, target)]
            return procs
                
        return list(walk(self, "_infrom")) + [self] + list(walk(self, "_into"))
    
    def proc_queue_list(self, also_show = []):
        return sorted([(p.name, p.queues_in, p.queues_out) for p in (self.all_procs() + also_show)])
        
    
    class Dog(F):
        def setup(self, n = 5., proc_queue_list = []):
            import pprint
            while not self.stopped():
                self.sleep(n/2.)
                print("WATCHDOG:", pprint.pformat(["Queues:"] + [(p[1].qsize(), p[2].qsize(), p[0]) for p in proc_queue_list]))
                self.sleep(n/2.)
        


class Delay(F):
    "Adds a c*(0,1) + b delay" 
    def setup(self, b = 0., c = 1.):
        self.b = b
        self.c = c
        
    def do(self, item):
        time.sleep(self.c * random.random() + self.b)
        self.push(item)


class StartSeq(F):
    "Adds a sequence id to meta"
    def setup(self, key = "seq_id"):
        self.key = key
        self.seq_id = 0
        
    def do(self, item):
        self.meta[self.key] = self.seq_id
        self.push(item)
        self.seq_id += 1


class EndSeq(F):
    "Orders input based on sequence id"
    def setup(self, key = "seq_id"):
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
                self.push(item)
                self.seq_id = self.seq_id + 1
            else:
                self.pq.put((p, data))
        

class MergeSeq(F):
    "Merges inputs into rows by sequence id"
    def setup(self, key = "seq_id", n = None):
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
                        self.push(keep)
                        self.seq_id += 1
                    else:
                        for item in keep:
                            self.pq.put((self.seq_id, item))
                            
                    for data in back:
                        self.pq.put(data)


class Batch(F):
    "Groups input into batches of 'size'"
    def setup(self, size = 64):
        self.batch = []
        self.size = size
        
    def do(self, item):
        
        self.batch.append(item)
        if len(self.batch) >= self.size:
            self.push(self.batch)
            self.batch = []
    
    def teardown(self):
        if len(self.batch) > 0:
            self.push(self.batch)





class Map(F):
    "Maps input -> output through function"
    def setup(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
    def do(self, item):
        self.push(self.fn(item, *self.args, **self.kwargs))


class Filter(F):
    "Filters input -> output by function"
    def setup(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
    def do(self, item):
        if self.fn(item, *self.args, **self.kwargs):
            self.push(item)





################
# Misc Helpers #
################

def flatten(to_flatten):
    """Given a list, possibly nested to any level, return it flattened."""
    # modified from http://code.activestate.com/recipes/578948-flattening-an-arbitrarily-nested-list-in-python/
    flattened = []
    for item in to_flatten:
        if isinstance(item, list):
            flattened.extend(flatten(item))
        else:
            flattened.append(item)
    return flattened
    
    
def listify(item = None):
    "Returns pretty much anything thrown at this as a list"
    if item is None:
        return []
    elif isinstance(item, list):
        return item
    elif isinstance(item, (set, tuple)):
        return list(item)
    elif isinstance(item, dict):
        return list(item.values())
    else:
        return [item]


# modified from http://blog.moertel.com/posts/2013-05-26-python-lazy-merge.html

def iterator_to_stream(iterator):
    """Convert an iterator into a stream (None if the iterator is empty)."""
    try:
        return next(iterator), iterator
    except StopIteration:
        return None

def stream_next(stream):
    """Get (next_value, next_stream) from a stream."""
    val, iterator = stream
    return val, iterator_to_stream(iterator)


def merge_iterators(iterators):
    """Make a lazy sorted iterator that merges lazy sorted iterators."""
    streams = list(map(iterator_to_stream, map(iter, iterators)))
    while streams:
        stream = streams.pop()
        if stream is not None:
            val, stream = stream_next(stream)
            streams.insert(0, stream)
            yield val
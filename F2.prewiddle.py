"""
F.py -- An experimental library of python multiprocessing.
"""

from multiprocessing import Process, JoinableQueue, Event
from queue import Empty, PriorityQueue


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
            self.name
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
        [q.join() for q in self.outputs]
        self.teardown()
        self.__done_event.set()
    
    # [Lifecycle] teardown(self)
    
    def stopped(self):
        return self.__stop_event.is_set()
    
    def finished(self):
        return self.__done_event.is_set()
    


class Sink(F):
    def do(self, item):
        pass


# just adds a Sink() on the end.
def EZ(*fns): #todo: rm ['-']
    return Wire(['-'] + list(fns) + [Sink()])


class Wire:
    
    def __init__(self, fns, qsize = 16, inputs = None, outputs = None):
        

        self.qsize = qsize
        self.watchdog = None
        
        if len(fns) == 0:
            raise ValueError("process functions are required")
        
        self.inputs = [] if inputs is None else inputs
        self.outputs = [] if outputs is None else outputs
        
        self.fns = self.link(fns)
        self.wire(self.graph())
    
    
    def graph(self):
        
        g = [self.mode] #.link sets mode. todo: kill it
        
        for fn in self.fns:
            if isinstance(fn, Wire):
                g.append(fn.graph())
            else:
                g.append(fn)
        return g
    

    def wire(self, g, incoming = []):
        mode = "-"
        if len(g) >= 2:
            
            mode = g[0]
            fn = g[1]
            
            if isinstance(fn, list):
                m = self.wire(fn, incoming)
                if m == "-":
                    self.wire([g[0]] + g[2:], [fn[-1]])
                else:
                    self.wire([g[0]] + g[2:], fn[1:])
                
            else:
                for e in self.endpoints(incoming, mode):
                    if e not in fn.infrom:
                        fn.infrom.append(e)
                
                if mode == '-':
                    self.wire([g[0]] + g[2:], [fn])
                else:
                    self.wire([g[0]] + g[2:], incoming)
        return mode
    
    def endpoints(self, g, mode):
        ends = []
        if len(g) >= 1:
            fn = g[0]
            if isinstance(fn, list):
                ends.extend(self.endpoints(fn[1:], fn[0]))
                ends.extend(self.endpoints(g[1:], mode))
            else:
                ends.append(fn)
                ends.extend(self.endpoints(g[1:], mode))
                """
                print(mode, g)
                if mode == '-':
                    if len(g) == 1:
                        ends.append(fn)
                    else:
                        ends.extend(self.endpoints(g[1:], mode))
                    
                else:
                    ends.append(fn)
                    ends.extend(self.endpoints(g[1:], mode))
                """
        
        return ends
    
    def link(self, fns):
        
        self.mode = '-'
        
        if isinstance(fns[0], str):
            self.mode = fns[0]
            fns = fns[1:]
        
        if self.mode == '-':
            return self.linkSerial(fns)
        elif self.mode == '|':
            return self.linkParallel(fns)
        elif self.mode == '*':
            return self.linkSplit(fns)
        else:
            raise ValueError("unknown link mode")
    
    def linkSerial(self, fns):
        
        for i in range(len(fns) - 1):
            
            fnA = fns[i]
            fnB = fns[i+1]
            
            if not isinstance(fnA, (F, Wire)):
                fnA = Wire(fnA, self.qsize)
                fns[i] = fnA
                
            if not isinstance(fnB, (F, Wire)):
                fnB = Wire(fnB, self.qsize)
                fns[i+1] = fnB
            
            if len(fnA.outputs) == 0 and len(fnB.inputs) == 0:
                q = JoinableQueue(self.qsize)
                fnA.outputs.append(q)    
                fnB.inputs.append(q)

            else:
                comb = list(set([*fnA.outputs, *fnB.inputs]))
                [l.clear() for l in [fnA.outputs, fnB.inputs]]
                fnA.outputs.extend(comb)
                fnB.inputs.extend(comb)
            
        self.inputs = fns[0].inputs
        self.outputs = fns[-1].outputs
        
        return fns
    
    def linkParallel(self, fns):
        
        for i in range(len(fns)):
            
            if not isinstance(fns[i], (F, Wire)):
                
                fns[i] = Wire(fns[i], self.qsize, self.inputs, self.outputs)
            
            fns[i].inputs = self.inputs
            fns[i].outputs = self.outputs
        
        return fns
        
    
    def linkSplit(self, fns):
        
        for i in range(len(fns)):
            
            if not isinstance(fns[i], (F, Wire)):
                fns[i] = Wire(fns[i], self.qsize, self.inputs, self.outputs)
                
            q = JoinableQueue(self.qsize)
            self.inputs.append(q)
            fns[i].inputs = self.inputs
            fns[i].outputs = self.outputs
        
        return fns

    
    def info(self):
        return [fn.info() for fn in self.fns]


    def printLayout(self, d = 0):
        for f in self.fns:
            if isinstance(f, Wire):
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
        self.watchdog = Wire.WatchDog(self, n)
        self.watchdog.start()
        return self
    
    def join(self):
        [fn.join() for fn in self.fns]
        
        if self.watchdog is not None:
            self.watchdog.stop()
        return self
    
    def stop(self):
        [fn.stop() for fn in self.fns]
        
    def stopped(self):
        return not any([not fn.stopped() for fn in self.fns])


    class WatchDog(F):
        def setup(self, link, n = 5.):
            import pprint
            while not self.stopped():
                self.sleep(n/2.)
                    
                
                print(pprint.pformat(link.info(), indent=4, width=80, depth=10, compact=False))
                self.sleep(n/2.)

def As(n = 1, fn = Sink, *args):
    return [fn(*args) for fn in range(n)]

    ##########


    """
    class Serial(list):
        pass
    
    class Parallel(set):
        pass
    
    class Broadcast(tuple):
        pass
    """


    ######

    """
    def put(self, item):
        [q.put(({}, item)) for q in self.inputs]
    
    def get(self):
        "Recieve some data"
        if len(self.inputs) == 0:
            "Nothing to see here"
            return None
        
        data = False
        while data is False and len(self.inputs) > 0:
            "This is a rickety rotary engine."
            
            rm_list = [] # Queues evaporate when they push None.
            data = False # False is not None
            
            for i, q in enumerate(self.inputs):

                try: 
                    data = q.get_nowait()
                except Empty:
                    continue
                
                if data is None:
                    rm_list.append(i)
                    data = False
                else:
                    break
            
            for i in sorted(rm_list, reverse=True):
                del self.inputs[i]
            
            sched_yield()

        if data is False:
            return None
        else:
            self.meta, item = data
            return item
    """
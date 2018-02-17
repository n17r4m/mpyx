from .F import F, SequenceStart, SequenceEnd
import random
from queue import PriorityQueue

# Public
# ======

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

def Zip(*fns):
    "Similar to the builtin zip() this will merge the results of sets"
    if len(fns) == 1:
        fns = fns[0]
    key = str(random.random())
    start = SequenceStart(key)
    merge = SequenceMerge(key, len(fns))
    
    return [start, set(fns), merge]




class Print(F):
    "A simple pass-through that prints out what it recieves"
    def setup(self, pre = None):
        self.pre = pre
    def do(self, item):
        print(item) if self.pre is None else print(self.pre, item)
        self.put(item)


class Stamp(F):
    "A simple debug counter to to track items in the workflow"
    def setup(self, pre = "Processing item" ):
        self.count = 0
        self.pre = pre
    def do(self, item):
        self.count += 1
        print(self.count) if self.pre is None else print(self.pre, self.count)
        self.put(item)



class Read(F):
    "Read a file line-by-line"
    def setup(self, filename, mode='r'):
        with open(filename, mode) as file:
            for data in file:
                self.put(data)
        self.stop()


class Write(F):
    "Write out to a file. Up to you to add new-lines"
    def setup(self, filename, mode='w'):
        self.file = open(filename, mode)
    
    def do(item):
        self.file.write(item)
        self.put(item)
    
    def teardown():
        self.file.close()


# Private
# =======

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



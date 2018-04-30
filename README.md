
mpyx
====

A python library for embarrassingly easy parallelism
----------------------------------------------------

The number of CPU cores in modern computers is increasing at a substantial rate.
For example, a high end server or workstation may now have up to 64 physical cores,
or up to 128 available threads!

See: https://www.amd.com/en/products/epyc-7000-series-2-socket-models

Unfortunately python is intrinsically limited by its single threaded design,
as well as the limitations imposed by the GIL (Global Interpreter Lock). 

The aim of this library is to break free of this limitation and make it super
easy to distribute nearly any workload across the full capabilities of your
system. In practice, one, or even two full orders of magnitude speed increase can
often be achieved with minimal code modification.


Background
----------

There are typically three ways to achieve parallelism in modern software development...

### Asynchronous / Event loops

This is the approach that node.js takes to achieve it's impressive speed, and
also has ~~first class~~ support available in python starting with version 3.5.

Ultimately, async code is still single-threaded, however by having multiple
tasks sitting on an "event loop," when a particular task (such as reading a
file, or fetching a resource from the internet) is blocked while performing IO,
other tasks may continue to run uninturrupted.

### Threads

Using threads allows a program to run multiple operations concurrently in the 
same process space. As mentioned above though, python is a bit quirky in that
it's most common implementation (CPython) implements a Global Interpreter Lock
which for most practical purposes results in only a single thread being executed
at any given time. The reason for this is to ensure memory safety, 
however it usually prevents work in python code operating concurrently. 

That said, many operations, such as most calls to a foreign function interface 
(e.g. most numpy functions) will allow the GIL to be released, so threads *can* 
improve performance beyond what asynchronous concurrancy can provide (which will
only concurrently run tasks which wait on blocking syscalls). 

### Multiprocessing

Process based parallelism is the granddaddy of concurrency and is the only way
to achieve real simultaneous utilization of multiple CPU cores in python. 
When using multiple processes, the parent process is forked into child processes 
which each have their own independant Global Interpreter Lock. Further, each 
child process is effectively isolated from each other, so there is no concerns 
about ensuring memory safety, however care must still be taken when accessing 
other shared resources, such as files or databases.

Typical multiprocessing in python requires a rather large amount of boilerplate
code. To be fair, it is still relatively straightforward:

```python
import multiprocessing as mp
import time
import random

def worker(num):
    """thread worker function"""
    time.sleep(random.random())
    print 'Worker:', num
    return


jobs = []
for i in range(5):
    p = mp.Process(target=worker, args=(i,))
    jobs.append(p)
[job.start() for job in jobs]
[job.join() for job in jobs]
    
```

Running this code will fork 5 child processes. On one test run, the following 
was printed:

```
Worker: 3
Worker: 0
Worker: 2
Worker: 1
Worker: 4
```

For a single long running task, this built in API is often sufficient, however
it very quickly becomes difficult and unwieldy to orchestrate complex 
workflows.


Introducing mpyx
----------------

The easiest way to start taking advantage of mpyx is to parallelise a sequence
of functions. Suppose you have a simple image processing pipeline that looks 
something like:

```python

# Before mpyx...

for image_file in imagesInDir("./original_pics"):
    image = imread(image_file)
    resized = resize(image)
    enhanced = enhance(resized)
    watermarked = watermark(enhanced)
    imsave(join("./site_pics/", image_file))
```

The trouble is that each image is being processed serially. Each image
is read, then resized, then enhanced, then watermarked, and then saved, in turn.
If you are running a website, or have to process folders with 1000's of images 
each, this could take a large amount of time. So, why not have each part of the 
process being run in parallel? Well, that's what mpyx is here for.

First let's introduce the most important tools in mpyx: `EZ` and `F`.
Here is a look at what the above workflow could be like using `EZ`:


```python

# with mpyx

from mpyx import EZ, Iter

EZ( 
    ImgFiles("./original_pics"),
    imread, resize, enhance, watermark,
    SaveTo("./site_pics")
).start()
```

`ImgFiles` and `SaveTo` are subclass instances of `F` which are explored
more later, but the meat of this example is that each function, `imread`, 
`resize`, `enhance`, `watermark`, are all operating in parallel now. 
Assuming each step takes about the same amount of CPU time, **this alone will 
give you a 5x speedup!**

### EZ

`EZ` is a wrapper for an internal class called `Indurate` (a latin word
meaning to strengthen, or harden, or make firm). The details of `Indurate` are
relatively unimportant, but what it does at a high level is set up a sequence
of `multiprocessing.JoinableQueue` between each part of your workflow.

There is a caveat however; suppose that most of the above image processing task 
is very quick, but resizing the image takes much longer than the other parts. 
As they say, a chain is only as strong as its weakest link.

To resolve this, `EZ` is able to recieve arbitrary nested data structures
that can represent virtually any combination of sequential, parallel, or 
broadcast/split data pipelines.

`[]` - lists represent sequential operations

`()` - tuples represent parallel operations

`{}` - sets represent broadcasted/split operations

Alternatively, (and required in some special circumstances where nested 
structures cannot be hashed), there are class wrappers that provide equivilant 
functionality, and can be intermixed with the above syntax.

```python
from mpyx import Serial, Parallel, Broadcast
# or
from mpyx import S, P, B # abbreviated aliases.
```

Continuing with the image processing example, to add real parallelism
to the resize step, the `EZ` statement could be rewritten as:

```python
EZ( 
    ImgFiles("./original_pics")), 
    (imread, imread, imread), 
    (resize, resize, resize, resize, resize, resize, resize), 
    (enhance, enhance, enhance),
    (watermark, watermark)
    (SaveTo("./site_pics"), SaveTo("./site_pics"))
).watch().start()
```

This will spawn a total of 17 processes and should give a huge speedup 
on the resize step, even though each image will still take a full second to roll 
through this hypothetical processing pipeline. 

Tuning the amount of parallelism at each step is a bit of an art, and
does require a bit of trial and error. Fortunately, by using the `watch()` tool,
it is easy to see in realtime how data is flowing throughout the computational
graph, where additional parallel processes should be added, and where 
existing ones are unnecessary.


As a final note, **you may embed the returned instance from `EZ` into other `EZ`
instances**, although a small amount of overhead is added by doing this.


### F

Although it is possible to use vanilla functions in a mpyx pipeline, `F` is the 
base class that can be extended from to provide advanced functionality during
the lifecycle of a child process. It provides useful hooks and methods to 
accomplish most needs:

```python
from mpyx import F

class YourTask(F):

    def initialize(self, *args, **kwargs):
        # This is executed very early on while still in the parent's process
        # context. Usually you won;t need to override this method, but can be
        # useful to perform some kinds of early configuration that cannot
        # be accomplished once this process is forked into a child.
        pass
        
    def setup(self, *args, **kwargs):
        # If your process needs to do any kind of setup once it has been forked,
        # or if it the first process in a workflow and expected to generate 
        # values for the rest of the pipeline, that code should go here.
        pass
        
    def do(self, item):
        # The main workhorse of a process. Items will flow in here, potentially
        # be modified, mapped, reduced, or otherwise morgified, and output can 
        # be then pushed downstream using the self.put() method. 
        # Here, for example, any items are simply passed along.
        self.put(item)
        
    def teardown(self):
        # Once there is no more data to process, the teardown() method is 
        # called. This is useful for commiting changes to a database, or 
        # performing necessary cleanup such as closing pipes or sockets.
        pass
```

Most complicated tasks will benifit from being declared as a subclass of `F`,
however as mentioned previously, if your task is a simple mapping of 
`foo(x) -> y`, you may use `foo` as a function in its vanilla state. 

An important gotchya is that `F` derived classes must be instantiated for use
within an `EZ` processing pipeline. This is to allow parameters to be set 
before the child process is forked.

Here is a complete example of a trivial pipeline for clarity:

```python

from mpyx import EZ, F, Print

class Count(F):
    def setup(self, to):
        for n in range(1, to+1):
            self.put(n)
        self.stop()

class AddX(F):
    def setup(self, amount=1):
        self.amount = amount
    def do(self, n):
        self.put(n + self.amount)

EZ(Count(5), AddX(10), Print()).start()
```

This will output

```
11
12
13
14
15
```


Cross Cutting Concerns
----------------------

*In aspect-oriented software development, cross-cutting 
concerns are aspects of a program that affect other concerns. These concerns 
often cannot be cleanly decomposed from the rest of the system in both the 
design and implementation, and can result in either scattering 
(code duplication), tangling (significant dependencies between systems), 
or both.* - Wikipeda

### .meta

Very often there are problems isolating each process to be completely 
independant from each other. In the image pipeline example, it would be useful
for the last child process `SaveTo` to know what the original file name was
from `ImgFiles` so that it could name the file correctly in the destination
folder.

Instances of `F` provide a special member property `meta` which is a `dict`
that will propogate through the `EZ` pipeline and are distinctly attached to a 
specific item. This is very useful when attaching ancillary information as required. 
Here are some potential concrete implementations of `ImgFiles` and `SaveTo` 
using `meta` to pass along the image filename:

```python

from pathlib import Path
from mpyx import F
from cv2 import imsave, imread

class ImgFiles(F):
    def setup(self, dir):
        for p in Path(dir).glob('*.jpg'):
            self.meta["fname"] = p.name
            self.put(p)
        self.stop()

class SaveTo(F):
    def setup(self, dir):
        self.dest = Path(dir)
    def do(self, image):
        imsave(self.dest / self.meta["fname"], image)

```

Note: Any modifications to `meta` must be done before `put` is called.

### .xcut

Sometimes a cross cutting concern requires actions to be performed at various
places within a pipeline that demand a shared context. Common instances of this
is opening and holding a single database transaction for the duration of the 
work, or sharing a logging facility between all the child processes. 
For this, mpyx offers the `xcut` method on instances of `Indurate` 
returned from `EZ`

`xcut` attaches a special queue on every child process within its computation
graph. This perhaps can be best explained with another code example:

```python

class DBWriter(F):
    def setup(self):
        self.conn = OpenDatabaseAndGetTransaction()
    def do(self, query)
        self.conn.query(*query)
    def teardown(self):
        self.conn.commit()
        self.conn.close()

class Ex_Task1(F):
    def do(self, item):
        # self.db is injected using .xcut
        self.db.put(("UPDATE stats SET seen = seen + 1 WHERE item = ?", item.id)) 
        # send item along to Ex_Task2
        self.put(item)

class Ex_Task2(F):
    def do(self, item):
        if some_check_if_item_is_valid(item):
            # self.db is injected here too.
            self.db.put(("UPDATE stats SET valid = True WHERE item = ?", item.id)) 
        self.put(item)

# Tip: xcut EZs will automatically start() when a dependant workflow is started. 
writer = EZ(DBWriter())
wflow = EZ(SomeDataSource(), Ex_Task1(), Ex_Task2())
wflow.xcut("db", writer)
wflow.start()

# or as a 1-liner
EZ(SomeDataSource(), Ex_Task1(), Ex_Task2()).xcut('db', EZ(DBWriter())).start()

```


### Manual wiring

In addition to `meta` and `xcut` there is also the option to manually create
instances of Queues, Events, and Pipes and supply them as arguments 
to your instantiated `F` objects during the `initialize` portion of the process 
lifecycle. 

For convinience, the following shortcuts are available on the F class object:

| Alias       | Maps to                         |
| ----------- | --------------------------------|
| `F.Queue`   | `multiprocessing.JoinableQueue` |
| `F.Event`   | `multiprocessing.Event`         |
| `F.Pipe`    | `multiprocessing.Pipe`          |



As and By
---------

For easy instantiation of many parallel processes two auxilery functions 
are provided by mpyx. `As` will instantiate several instances of a subclass
of `F` in *parallel* mode, and `By` will do the same, but in *Broadcast* mode.

```python

from mpyx import EZ, As, F

class SomeTask(F):
    def setup(self, arg1, arg2):
        self.arg1, self.arg2 = arg1, arg2
    def do(self, item):
        # do work on item


EZ(SomeDataSrc(), As(8, SomeTask, "arg1", "arg2")).start()
```



Built in Wrappers
-----------------

Many common operations have been included within the mpyx module and may 
be imported directly by name.


```python

Const(item, limit = None)   
"A constant value generator"

Iter(iterable)
"A simple iterator that flows an input iterable into the process graph"

Filter(fn, *args, **kwargs)
"Filters input -> output by function"

Map(fn, *args, **kwargs)
"Maps input -> output by function"

Batch(size = 64)
"Groups input into batches of 'size'"

Zip(*fns)
"Similar to the builtin zip() this will merge the results of broadcasted sets"
"into a zipped array"

Print(prefix = None)
"A simple pass-through that prints out what it recieves"
        
Stamp(pre = "Processing item")
"A simple debug counter to to track items in the workflow"

Read(filename, mode='r')
"Read a file line-by-line"

Write(filename, mode='w'):
"Write out to a file. Up to you to add new-lines"

```

In addition, there are some work-in-progress extensions that you may find useful.


```python

from mpyx.Vid import FFmpeg

FFmpeg(input, input_opts, output, output_opts, global_opts, verbose=False)
"Opens ffmpeg for reading/writing or streaming."
"If a shape (e.g. (800, 600, 3) )" is provided for input and/or output,"
"then it will stream through pipes, otherwise it will read/write to file."

```


API Refrence
------------


### F

#### Lifecycle
```python


class YourTask(F):
    def initialize(self, *args, **kwargs):
        # pre-fork()
        # cannot use self.put()
        
    def setup(self, *args, **kwargs):
        # post-fork()
        # can use self.put()
        
    def do(self, item):
        # received input
        self.put(item)
        
    def teardown(self):
        # all predecessors finished, input queue drained.
```

#### Available methods 


##### self.put(data)

Puts an item into the outgoing queue

##### [fn|self].stop()

End the process ASAP. Commonly used within `.setup` to demarcate the end of new
data and end the process.

##### self.async(coroutine())

Helper function to await on an async function.

##### self.myAsync(coroutine

Helper function to call and await on an async function.

##### self.sleep(seconds)

Pauses the function for a little while

##### [fn|self].started()

Returns whether this process has started yet or not.

##### [fn|self].stopped()

Returns whether this process has started shutdown yet or not.

##### [fn|self].finished()

Returns whether this process has completed and exited yet or not.


### EZ / Indurate

A few example invocations

```python

EZ(Tasks(), ToRun(), Serial(), Mode())

EZ([Also(), Tasks(), Serial()])

EZ([Start(), (Parrallel(), Parrallel()), End())

EZ(First(), {BCast(), BCast()}, ThisGets2xItemsAsFirstGenerates())

EZ(Data(), {DoSomethingWithData(), 
    [DoSomethingElseWithData(), AndThenThis()]})

EZ(Data(), {MaybeLog("logdir"), 
    [GotData(), 
     As(24, MassiveParrallel), 
     As(12, ResolveSomething, 'arg1')], 
    {LogAfterProcs(), Cleanup()}})

```

#### API Reference

##### e.start()

Starts all the child processes and begins work.

##### e.join()

Block until all work has completed. Will call .start() if not already started.

##### e.stop()

Ends all processes in the computation graph, 
even if they have not finished working.

##### e.stopped()

Returns whether all child processes are in the .stopped() state.

##### e.items()

Returns a lazy generator that emits any output items from the computation.

##### e.list()

Same as .items(), but waits until the computation is finished and returns 
a concrete list of results.

##### e.watch()

Starts a watchdog process that prints to stdout the current queue sizes. Useful 
for determining where bottlenecks are located (and where you should add more parralelism :)

##### e.unwatch()

Stops a previous started watchdog.

##### e.graph()

Return a nested interpretation of the current computation graph.

##### e.printLayout()

Pretty Prints the current computational graph. [TODO: Document format]

##### e.daemonize()

Sets `daemon = True` on all child processes. Must be called before starting.


Internals
---------


### EZ Wiring

Every instance of `F` has the following important properties which manage how it
behaves relative to its location in the computational graph. These properties
should be considerd as private, however they are available for inspection if
required.

`.inputs` - An array that holds references to Queues that could emit items
into this processes' `do(item)` method. 

`.outputs` - An array that holds references to Queues that will be added to if
`self.put(item)` is called.

`.infrom` - An array that holds references to immediate predecessors in the 
computational graph. 

`EZ` will traverse its given data structure and appropriately connect each 
process to their predecessors and ancestors while respecting the semantics
of sequential, parallel, or broadcast processing.

Care should be taken, as during construction of the graph by `EZ` each
nested collection will instantiate its own `Indurate`. To correctly pipe
inputs and outputs into the worker processes, shared instances of the 
`.inputs` and `.outputs` arrays may be created. Specifically, the `Indurate`
instances and the `F` instances may sometimes share a reference to the 
*same* array. As a consequence, reassigning a worker processes' `.inputs` or 
`.outputs` will probably cause unwanted and undefined behaviour.


### F Lifecycle

For reference, the complete lifecycle of a `F` instance is as follows: 

Methods that are executed in the parents process with #P and methods executed
within the child with #C

Methods intended to be overridden are marked with a *

Methods intended to be called by parent as part of control flow are marked with !


```python

__init__(self, *args, **kwargs)   #P

initialize(self, *args, **kwargs) #P *
    
start(self)                       #P !

__beforeStart(self)               #P

run(self)                         #C

__afterStart(self)                #P

setup(self, *args, **kwargs)      #C *

do(self, item)                    #C *

join(self)                        #P !

stop(self)                        #P !

shutdown(self)                    #C

teardown(self)                    #C *
    

```





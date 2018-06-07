#!/usr/bin/python

from F import EZ, As, By, F, Map, Seq
from F import Serial, Parallel, Broadcast, S, P, B
from util import Iter, Const, Print, Stamp, Filter, Batch, Zip, Read, Write


# Example Process Function
class Add(F):

    def initialize(self, val=0):
        self.val = val

    def do(self, item):
        self.put(item + self.val)

    def teardown(self):
        pass


# Set up a simple data source.
def Src(n=5):
    return Iter(range(1, (n + 1)))


# Print info for each demo
def demo(description, ez):
    print("\n")
    print(description)
    print("\n")
    ez.printLayout()
    print(ez.graph())
    # ez.watch(0.1)
    ez.start().join()


demo("Sanity.", EZ(Src(), Print("Serial")))

demo("Generate constant values with Const", EZ(Const(0, 5), Add(1), Print()))

demo(
    "In-Series processing uses [] or Serial() or S().",
    EZ([Src(), Print("Also Serial")]),
)

demo(
    "Parallel Processing uses () or Parallel() or P().",
    EZ(Src(), (Print("A"), Print("B"))),
)

demo(
    "Broadcast Processing uses {} or Broadcast() or B().",
    EZ(Src(), {Print("A"), Print("B")}),
)

demo(
    "Mix [], (), and {} with S(), P() and B() if you get a hash error.",
    EZ(Src(), P([Add(1000), Add(1000)], [Add(100), Add(100)]), Print()),
)

demo(
    "[] cannot be hashed. () and {} are ok.",
    EZ(Src(), (Add(1000), Add(5000)), (Add(100), Add(500)), Print()),
)


def adder(x, y=10):
    return x + y


demo("Bare functions act as f(x) -> y maps", EZ(Src(), adder, Print()))

demo(
    "Use Map to paramatize a bare function or lambda",
    EZ(Src(), Map(adder, 100), Map(lambda x: x - 50), Print()),
)

demo("Filter items out", EZ(Src(), Filter(lambda x: x > 2), Print()))


demo("Batch items together for handy group processing.", EZ(Src(), Batch(2), Print()))

demo("Use As() to go really parallel.", EZ(Src(), As(5, Add, 10), Print()))


demo(
    "Use Seq() to preserve ordering during parallel execution.",
    EZ(Src(), Seq(As(5, Add, 10)), Print()),
)

demo("Use By() to do wide broadcasts.", EZ(Src(2), By(5, Add, 10), Print()))


demo(
    "Use Zip() to merge broadcasted results into a group in-order.",
    EZ(Src(), Zip(Add(10), Add(100)), Print()),
)


print(
    "\n\nOf course, it is easy to get results from the process chain using .items\n\n"
)

for item in EZ(Src(), Add(5)).items():
    print("Item:", item)


print("\n\nOr use .list (actually is just list(ez.items()) ).\n\n")

fived = EZ(Src(), Add(5)).list()
print("Five:", fived)


import time

before = time.perf_counter()

demo(
    "And now for a big ugly pile of pachinko fun",
    EZ(
        Const(0, 5),
        (Add(1), Add(10), Add(100), Add(1000), Add(10000)),
        (Add(1), Add(10), Add(100), Add(1000), Add(10000)),
        (Add(1), Add(10), Add(100), Add(1000), Add(10000)),
        (Add(1), Add(10), Add(100), Add(1000), Add(10000)),
        (Add(1), Add(10), Add(100), Add(1000), Add(10000)),
        (Add(1), Add(10), Add(100), Add(1000), Add(10000)),
        (Add(1), Add(10), Add(100), Add(1000), Add(10000)),
        (Add(1), Add(10), Add(100), Add(1000), Add(10000)),
        Print(),
    ),
)

print("Took {} ms to run ~45 processes".format((time.perf_counter() - before) * 1000))


print("\n\nCan also read and write files.\n\n")
print("".join(EZ(Read("./LICENSE.md")).list()))


print("\n\nLet's try cross cutting between workflows")


class Worker(F):

    def setup(self, name):
        self.name = name

    def do(self, item):
        res = item * 10
        self.put(res)
        self.logger.put(("Hello from", self.name, "I put", res))


EZ(Src(), (Worker("1"), Worker("2"))).xcut("logger", EZ(Print())).start().join()


e1 = EZ(Src())
e2 = EZ(Print())
print("\n\nWhat happens when we try and merge to instances of Induration?")
e3 = EZ(e1, e2)
e3.printLayout()
print(e3.graph())
e3.start().join()


print("Exception handling")


class ErrorSetup(F):

    def setup(self):
        raise AttributeError("A Error")


class ErrorDo(F):

    def do(self, item):
        raise ValueError("A Error")


class ErrorTeardown(F):

    def teardown(self):
        raise KeyError("A Error")


def e_handler(e, tb):
    print("Exception occured:", e)
    print(tb)


e1 = EZ(Src(), ErrorDo(), Print())


e1.catch(e_handler).start().join()


print("All done!")

"""
print("Let's try reading and writing some video!")


from mpyx.Vid import FFmpeg
import numpy as np

class StaticFrameGenerator(F):
    def setup(self, shape):
        for _ in range(20):
            self.put(np.random.randint(0, 255, shape, dtype="uint8"))
        self.stop()


frame_shape = (1920, 1080, 3)

EZ(
    StaticFrameGenerator(frame_shape), 
    FFmpeg(frame_shape, [], "test.avi", ['-c:v libx264 -preset slow -crf 22'], ["-y"])
).start().join()


print("Let's try a more complicated ffmpeg pipeline.")



#import cv2

class Intensify(F):
    def do(self, frame): 
        
        frame_mod = (127 + frame / 2).astype('uint8')
        #cv2.imshow("show", frame_mod)
        #cv2.waitKey(1)
        
        self.put(frame_mod)
        


EZ(
    FFmpeg("test.avi", [], (1920,1080,3), []),
    Stamp(),
    FFmpeg((1920,1080,3), [], (1280,720,3), ["-vf scale=1280:720"]),
    Intensify(),
    FFmpeg((1280,720,3), [], "test2.avi", ['-c:v libx264 -preset slow -crf 22'], ["-y"])
).start().join()



print("Transcode from file to file.")


EZ(
    FFmpeg("test2.avi", [], "test3.avi",  ['-c:v libx264 -preset slow -crf 30'], ["-y"])
).start().join()

"""


""" Current works up to here.... maybe.. continue here
### todo, remove ascii control, [the '-', '|', '*', stuff.]
### use [] for series
### use {} for parallel (note: non indexible. Perfect!), 
### and () for broadcast (note: immutable. Perfect!).

# so, must adhere to no indexing, and immutability.
# refactor hasn't happened yet.


# P.S. current interface looks like.. (  [] = '-', {} = '|', and () = '*',
    as the first element of the list)

l = EZ(
    ['-', 
        ['-', Itr(range(5)), ['|', Print("A"), Print("Z")]],
        ['|', Print("B"), ['|', Print("C"), Print("X")]],
        ['-', Print("D")]
    ]
)

"""

# demo("Parallel Processing uses {}.",
#    EZ(Src(), {Print("~50% Parallel Fn A"), Print("~50% Parallel Fn B")}))


"""
l = EZ((Itr(range(10)), Itr(range(20,30))), Print("0-9 and 20-29, ooo"))
l = EZ(Src(), (Print("A"), Print("B")))
l = EZ(Src(), {Mul(10), Mul(10), Mul(10)}, Print("x10"))

"""


"""
l = EZ(
    ['-', 
        ['-', Itr(range(5)), ['|', Print("A"), Print("Z")]],
        ['|', Print("B"), ['|', Print("C"), Print("X")]],
        ['-', Print("D")]
    ]
)
"""


async def main(*args):
    pass

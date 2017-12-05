Ez-Pz-Yx ?! Processes.

Caution: alpha.

Example:

    import mpyx
    
    (   SomeDataSource(100000)
        .into(FirstStageProcessing())
        .into(SecondaryProcessing())
        .print()
        .into(StarryNight())
    ).execute()
    
    class SomeDataSource(mpyx.F):
        def setup(self, n = 100): [self.push(i) for range(n)]            # Generator
        
    class FirstStageProcessing(mpyx.F):
        def do(self, i): self.push(i * 100)                           # FunctionNode
    
    class SecondaryProcessing(mpyx.F):
        def do(self, i100): 
            self.sleep(1)                                             # FunctionNode
            self.push(i100)
    
    class StarryNight(mpyx.F):
        def teardown(self):
            print("Fin")
            


Background

Basically, It would be really sweet to just pipe python processes around.

So, first we have to import something, because explicit is better than implicit.

    import mpyx

mpyx exports a few useful things: 
###### TODO ###### setup mpyx/__init__.py in a good way.

from Process import F
"A mp.Process process by any other name would not smell so sweet."

An instance from the F class has the following useful properties:

    class YourProcess(F):
        def initialize(self, *args, **kwargs):
            pass
        def setup(self, *args, **kwargs):
            pass
        def do(self, item):
            self.push(item)
        def teardown(self):
            pass
            





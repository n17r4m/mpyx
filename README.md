Ez-Pz-Yx ?! Processes.

Caution: alpha.



mpyx exports a few useful things: 
###### TODO ###### setup mpyx/__init__.py in a good way.

from Process import F

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
            





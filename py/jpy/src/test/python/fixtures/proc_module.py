
def initialize():
    return 'initialize'

def computeTile(x, y, array):
    spend_some_time()
    return 'computeTile-' + str(x) + ',' + str(y)

def dispose():
    return 'dispose'


def spend_some_time():
    l = list(range(10000))
    for i in range(10000):
        l.reverse()

_module_val = None
def setVal(val):
    global _module_val
    _module_val = val
    
def getVal():
    global _module_val
    return _module_val

def check1234():
    global _module_val
    return _module_val == 1234

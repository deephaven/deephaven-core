class Simple(object):
    def __init__(self, v):
        self._v = v
        
    def __str__(self):
        return "Simple: " + str(self._v)
    
    def getValue(self):
        return self._v
    
class HashSimple(object):
    def __init__(self, v):
        self._v = v
        
    def __str__(self):
        return "HashSimple: " + str(self._v)

    def getValue(self):
        return self._v

    def __hash__(self):
        return hash(self._v)
    
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._v == other._v
        else:
            return False  # Java can't support NotImplemented
        

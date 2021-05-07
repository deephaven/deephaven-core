class Simple:
    def __init__(self, x):
        self.x = x

    def __hash__(self):
        return hash(self.x)

    def __str__(self):
        return str(self.x)

    def __eq__(self, other):
        return self.x == other

class ThrowsException:
    def __hash__(self):
        raise Exception('this __hash__ always raises Exception')

    def __str__(self):
        raise Exception('this __str__ always raises Exception')

    def __eq__(self, other):
        raise Exception('this __eq__ always raises Exception')

# __eq__ *can* return non boolean values
class NonBooleanEq:
    def __eq__(self, other):
        if self is other:
            return 1
        return 0

# __str__ should *not* return non string values
class NonStringStr:
    def __str__(self):
        return 1

class NoMethodsDefined:
    def __init__(self):
        pass

class EqAlwaysFalse:
    def __eq__(self, other):
        return False

class EqAlwaysTrue:
    def __eq__(self, other):
        return True

class HashNegativeOne:
    def __hash__(self):
        return -1
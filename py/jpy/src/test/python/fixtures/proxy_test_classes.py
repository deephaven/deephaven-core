class HasStr:
    def __str__(self):
        return 'This is my __str__ method'

class DoesNotHaveStr:
    def __init__(self):
        pass

class HasToStringWithArg:
    def toString(self, arg):
        return 'weird'

class HasHashCodeWithArg:
    def hashCode(self, arg):
        return 0

    def __hash__(self):
        return 1

class HasHashNegativeOne:
    def __hash__(self):
        return -1

class HasEqualsWithoutArg:
    def equals(self):
        return True

class EqAlwaysFalse:
    def __eq__(self, other):
        return False

class EqAlwaysTrue:
    def __eq__(self, other):
        return True

class NoEqDefined:
    def __init__(self):
        pass

class ProxyAsArgument:
    def self_is_other(self, other):
        return self is other

class ThrowsException:
    def __hash__(self):
        raise Exception('this __hash__ always raises Exception')

    def __str__(self):
        raise Exception('this __str__ always raises Exception')

    def __eq__(self, other):
        raise Exception('this __eq__ always raises Exception')

class NonBooleanEq:
    def __eq__(self, other):
        if self is other:
            return 1
        return 0
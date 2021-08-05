import jpy
import wrapt
import numpy as np
import torch as pt
import tensorflow as tf
import pandas as pd
import unittest

_called_ = False
# None until the first _defineSymbols() call
_CallPyFunc_ = None


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _called_, _CallPyFunc_  # , _Scatterer_
    if not _called_:
        _called_ = True
        _CallPyFunc_ = jpy.get_type("io.deephaven.integrations.learn.CallPyFunc")


# every module method that invokes Java classes should be decorated with @_passThrough
@wrapt.decorator
def _passThrough(wrapped, instance, args, kwargs):
    """
    For decoration of module methods, to define necessary symbols at runtime

    :param wrapped: the method to be decorated
    :param instance: the object to which the wrapped function was bound when it was called
    :param args: the argument list for `wrapped`
    :param kwargs: the keyword argument dictionary for `wrapped`
    :return: the decorated version of the method
    """

    _defineSymbols()
    return wrapped(*args, **kwargs)


def identity(*args):  # returns a tuple
    return args


def addOne(*args):
    return [arg + 1 for arg in args]  # returns a list


def mySum(*args):
    return sum(args)  # returns a numeric


def npArray(*args):
    return np.array(args)


def tfTensor(*args):
    return tf.constant(args)


def ptTensor(*args):
    return pt.tensor(args)


def pdDataFrame(*args):
    return pd.DataFrame(args)


@_passThrough
class TestPassThrough(unittest.TestCase):

    def identityTest1(self):
        callPyFunc = _CallPyFunc_(identity, 1, 2, 3, 4, 5)
        self.assertEqual(callPyFunc.call(), (1, 2, 3, 4, 5))

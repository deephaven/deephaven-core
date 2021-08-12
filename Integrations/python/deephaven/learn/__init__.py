#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""
Deephaven's learn module enables the use of libraries like PyTorch and Tensorflow in conjunction with Deephaven tables
for efficient real-time data work.

Classes:

    Input: Facilitates data transfer from Deephaven tables to Python objects like PyTorch or Tensorflow tensors.
    Output: Facilitates data transfer from Python objects that are returned from PyTorch or Tensorflow to Deephaven tables.

Functions:

    eval(table, model_func, live, inputs, outputs):
        Takes relevant data from Deephaven table using inputs, converts that data to the desired Python type, feeds
        it to model_func, and stores that output in a Deephaven table using outputs.

        :param table: the Deephaven table to perform computations on
        :param model_func: the function that takes in data and performs AI computations. NOTE that model_func must have
                           'target' and 'features' arguments in that order
        :param inputs: the list of Input objects that determine which columns get extracted from the Deephaven table
        :param outputs: the list of Output objects that determine how to store output from model_func into the Deephaven table
"""

import jpy
import wrapt
from deephaven import npy
from deephaven import listen
import numpy as np
import torch
import tensorflow as tf

_called_ = False
_Input_ = None
_Output_ = None
_Computer_ = None
_Scatterer_ = None


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _called_, _Input_, _Output_, _Computer_, _Scatterer_
    if not _called_:
        _called_ = True
        _Input_ = jpy.get_type("io.deephaven.integrations.learn.Input")
        _Output_ = jpy.get_type("io.deephaven.integrations.learn.Output")
        _Computer_ = jpy.get_type("io.deephaven.integrations.learn.Computer")
        _Scatterer_ = jpy.get_type("io.deephaven.integrations.learn.Scatterer")


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


@_passThrough
class Input:
    """
    Provides an interface for getting data from Deephaven tables to Python objects that popular deep learning libraries
    know how to use. Avoids many inefficencies of Python and allows for seamless integration of Deephaven's real-time
    capabilities with modern AI frameworks.
    """
    def __init__(self, colNames, gatherFunc):
        self.input = _Input_(colNames, gatherFunc)


@_passThrough
class Output:
    """
    Provides an interface for getting data from Python objects like Tensorflow Tensors into Deephaven tables. Enables
    simple but powerful user-configurability via the scatter function, and works with Deephaven's native real-time capabilities.
    """
    def __init__(self, colName, scatterFunc, type=None):
        self.output = _Output_(colName, scatterFunc, type)


@_passThrough
def eval(table=None, model_func=None, inputs=[], outputs=[], batch_size = None):
    """
    Takes relevant data from Deephaven table using inputs, converts that data to the desired Python type, feeds
    it to model_func, and stores that output in a Deephaven table using outputs.

    :param table: the Deephaven table to perform computations on
    :param model_func: the function that takes in data and performs AI computations. NOTE that model_func must have
                       'target' and 'features' arguments in that order
    :param inputs: the list of Input objects that determine which columns get extracted from the Deephaven table
    :param outputs: the list of Output objects that determine how to store output from model_func into the Deephaven table
    """

    # reminder to do error checking on the set of inputs and the set of outputs

    if batch_size == None:
        if table.isLive():
            raise ValueError("Batch size cannot be inferred on a live table. Please specify a batch size.")
        batch_size = table.size()

    #computer = _Computer_(table, model_func, [input.input for input in inputs], batch_size)
    globals()["computer"] = _Computer_(table, model_func, [input.input for input in inputs], batch_size)

    if not outputs == None:
        #scatterer = _Scatterer_([output.output for output in outputs])
        globals()["scatterer"] = _Scatterer_([output.output for output in outputs])

        return table.update("FutureOffset = computer.compute(k)", "Clean = computer.clear()").update(scatterer.generateQueryStrings("FutureOffset")).dropColumns("FutureOffset","Clean")

    return table.update(["FutureOffset = computer.compute(k)", "Clean = computer.clear()", "Result = FutureOffset.getFuture().get()"]).dropColumns("FutureOffset","Clean","Result")

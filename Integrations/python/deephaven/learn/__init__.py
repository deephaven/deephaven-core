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
from deephaven import QueryScope
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


class Input:
    """
    Provides an interface for getting data from Deephaven tables to Python objects that popular deep learning libraries
    know how to use. Avoids many inefficencies of Python and allows for seamless integration of Deephaven's real-time
    capabilities with modern AI frameworks.
    """
    def __init__(self, colNames, func):
        self.colNames = colNames
        self.func = func


class Output:
    """
    Provides an interface for getting data from Python objects like Tensorflow Tensors into Deephaven tables. Enables
    simple but powerful user-configurability via the scatter function, and works with Deephaven's native real-time capabilities.
    """
    def __init__(self, colNames, func, type):
        self.colNames = colNames
        self.func = func
        self.type = type


def _parseInput(inputs, table):
    """
    Converts the list of user inputs into a new list of inputs with the following rules:

        inputs = [Input([], gather)]
        will be transformed into a list containing a new Input object, with every column in the table as an element in
        Input.columns. This allows users to not have to type all column names to use all features.

        inputs = [Input(["target"], gather), Input([], gather)]
        will be transformed into a list containing two Input objects; the first will be unchanged and represent the
        target variable, the second will be transformed to an Input object containing all column names in the dataset
        except for the target. This allows users to not have to type all column names to use all features.

        If inputs is of length 2 or greater, we assume that the first Input object is the target variable and insist
        that it be non-empty.

    :param inputs: the list of Input objects that gets passed to eval()
    :param table: the Deephaven table that gets passed to eval()
    """
    new_inputs = inputs

    if len(inputs) == 0:
        raise ValueError('The input list cannot have length 0.')

    elif len(inputs) == 1:

        if not isinstance(inputs[0], Input):
            raise TypeError('Please use the Input class provided with learn to pass input.')

        if len(inputs[0].colNames) == 0:
            new_inputs[0].colNames = list(table.getMeta().getColumn("Name").getDirect())
            return new_inputs

        else:
            if not all(isinstance(col, str) for col in inputs[0].colNames):
                raise TypeError('Input column lists may only contain strings.')

            return new_inputs

    else:

        if not all(isinstance(input, Input) for input in inputs):
            raise TypeError('Please use the Input class provided with learn to pass input.')

        if len(inputs[0].colNames) == 0:
            raise ValueError('Target input cannot be empty.')

        else:

            target = inputs[0].colNames

            if not isinstance(target[0], str):
                raise TypeError('Target must be a string.')

            for i in range(1,len(inputs)):

                if len(inputs[i].colNames) == 0:
                    new_inputs[i].colNames = list(table.dropColumns(target).getMeta().getColumn("Name").getDirect())

                else:
                    if target[0] in inputs[i].colNames:
                        raise ValueError('Target column cannot be present in any other input.')

                    if not all(isinstance(col, str) for col in inputs[i].colNames):
                        raise TypeError('Input column lists may only contain strings.')

                    pass

            return new_inputs


@_passThrough
def _toJavaIn(inputs, table):
    """
    Converts a list of Python Input objects, each with possibly multiple entries, to a list of Java Input objects.

    :param output: the list of Python Input objects to be converted to Java Input objects
    """
    inputs = _parseInput(inputs, table)
    newInputs = []
    # for every input object, convert to Java Input object and append
    for input in inputs:
        newInputs.append(_Input_(input.colNames, input.func))

    return newInputs


@_passThrough
def _toJavaOut(output):
    """
    Converts a Python Output object with multiple columns to a list of Java Output objects with singular columns.

    :param output: the Python Output object to be converted to a series of Java Output objects
    """
    newOutputs = []
    # for every column given, pull it out and create an output object with the same scatter function, data type
    for column in output.colNames:
        newOutputs.append(_Output_(column, output.func, output.type))

    return newOutputs


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
    # set batch to be number of rows in whole table if batch size is not provided
    if batch_size == None:
        if table.isLive():
            raise ValueError("Batch size cannot be inferred on a live table. Please specify a batch size.")
        batch_size = table.size()

    computer = _Computer_(table, model_func, batch_size, *_toJavaIn(inputs, table))
    QueryScope.addParam("computer", computer)

    if not outputs == None:
        scatterer = _Scatterer_(*[jOutput for pyOutput in outputs for jOutput in _toJavaOut(pyOutput)])
        QueryScope.addParam("scatterer", scatterer)

        return table.update("FutureOffset = computer.compute(k)", "Clean = computer.clear()").update(scatterer.generateQueryStrings()).dropColumns("FutureOffset","Clean")

    return table.update("FutureOffset = computer.compute(k)", "Clean = computer.clear()", "Result = FutureOffset.getFuture().get()").dropColumns("FutureOffset","Clean","Result")
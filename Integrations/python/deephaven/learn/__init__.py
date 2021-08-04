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
        :param live: indicates whether to treat your computation as a live computation. NOTE that this is not asking if
                     your table is live or not, but whether model_func should be called at every table update. For instance,
                     when performing training on a live dataset, you do not want to re-train at every table update
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
# None until the first _defineSymbols() call
_IndexSet_ = None
_Future_ = None
_Computer_ = None
_CallPyFunc_ = None
_Scatterer_ = None


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _called_, _IndexSet_, _Future_, _Computer_, _CallPyFunc_, _Scatterer_
    if not _called_:
        _called_ = True
        _IndexSet_ = jpy.get_type("io.deephaven.integrations.learn.IndexSet")
        _Future_ = jpy.get_type("io.deephaven.integrations.learn.Future")
        _Computer_ = jpy.get_type("io.deephaven.integrations.learn.Computer")
        _CallPyFunc_ = jpy.get_type("io.deephaven.integrations.learn.CallPyFunc")
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


Input = jpy.get_type("io.deephaven.integrations.learn.Input")
Output = jpy.get_type("io.deephaven.integrations.learn.Output")


# could be that this should be broken up into two pieces, one to check errors and one to transform input?
def _parse_input(inputs, table):
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
    # what are all possible cases
    new_inputs = inputs
    # input length zero - problem
    if len(inputs) == 0:
        raise ValueError('The input list cannot have length 0.')
    # first input list of features
    elif len(inputs) == 1:
        # ensure input class is used
        if not isinstance(inputs[0], Input):
            print(type(inputs[0]))
            print(type(Input))
            raise TypeError('Please use the Input class provided with learn to pass input.')
        # if list of features is empty, replace with all columns and return
        if len(inputs[0].columns) == 0:
            new_inputs[0].columns = list(table.getMeta().getColumn("Name").getDirect())
            return new_inputs
        else:
            # ensure there are only strings in the given column set
            if not all(isinstance(col, str) for col in inputs[0].columns):
                raise TypeError('Input column lists may only contain strings.')
            return new_inputs
    else:
        # verify all input is of type Input
        if not all(isinstance(input, Input) for input in inputs):
            print(type(inputs[0]))
            print(type(Input))
            raise TypeError('Please use the Input class provided with learn to pass input.')
            print("not is all")
        # now that we know input length at least 2, ensure target non-empty
        if len(inputs[0].columns) == 0:
            raise ValueError('Target input cannot be empty.')
        else:
            target = inputs[0].columns
            # ensure target is a string
            if not isinstance(target[0], str):
                raise TypeError('Target must be a string.')
            # look through every other input to find empty list
            for i in range(1,len(inputs)):
                # if empty list found, replace with list of all column names except for target
                if len(inputs[i].columns) == 0:
                    new_inputs[i].columns = list(table.dropColumns(target).getMeta().getColumn("Name").getDirect())
                else:
                    # ensure target does not appear in any other column list
                    if target[0] in inputs[i].columns:
                        raise ValueError('Target column cannot be present in any other input.')
                    # ensure there are only strings in the given column set
                    if not all(isinstance(col, str) for col in inputs[i].columns):
                        raise TypeError('Input column lists may only contain strings.')
                    pass
            return new_inputs

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
        batch_size = table.size()

    # set output to be an empty output object if none
    if outputs == None:
        outputs = Output(None, None, None)

    computer = _Computer_(model_func, table, batch_size, *inputs)
    scatterer = _Scatterer_(batch_size, outputs)

    QueryScope.addParam("computer", computer)
    QueryScope.addParam("scatterer", scatterer)

    return table.update("Future = computer.compute(k)", "Clean1 = computer.clear()", f"{outputs.getColNames()[0]} = scatterer.scatter(Future.get(), Future.getOffset())").dropColumns("Future", "Clean1")

#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""
Deephaven's learn module enables the use of libraries like PyTorch and Tensorflow in conjunction with Deephaven tables
for efficient real-time data manipulation, modelling, and forecasting.

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
        :param batch_size:
"""

import jpy
import wrapt

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

    global _Input_, _Output_, _Computer_, _Scatterer_
    if _Input_ is None:
        _Input_ = jpy.get_type("io.deephaven.integrations.learn.Input")
        _Output_ = jpy.get_type("io.deephaven.integrations.learn.Output")
        _Computer_ = jpy.get_type("io.deephaven.integrations.learn.Computer")
        _Scatterer_ = jpy.get_type("io.deephaven.integrations.learn.Scatterer")


# every module method that invokes Java classes should be decorated with @_passThrough
@wrapt.decorator
def _passThrough(wrapped, instance, args, kwargs):
    """
    For decoration of module methods, to define necessary symbols at runtime

    Args:
        wrapped  (type): the method to be decorated
        instance (type): the object to which the wrapped function was bound when it was called
        args     (type): the argument list for `wrapped`
        kwargs   (type): the keyword argument dictionary for `wrapped`

    Returns:
        type: the decorated version of the method.
    """
    _defineSymbols()
    return wrapped(*args, **kwargs)


@_passThrough
class Input:
    """
    Input specifies how to gather data from a Deephaven table into a Python object.

    Attributes:
        input (io.deephaven.integrations.Input):
    """
    def __init__(self, colNames, gatherFunc):
        """
        Args:
            colNames  (:obj:`list` of :obj:`str` or str): Column name or list of column names
        """
        self.input = _Input_(colNames, gatherFunc)

    def __str__(self):
        return "Columns: " + str([self.input.getColNames()[i] for i in range(len(self.input.getColNames()))])


@_passThrough
class Output:
    """
    Provides an interface for getting data from Python objects like Tensorflow Tensors into Deephaven tables. Enables
    simple but powerful user-configurability via the scatter function, and works with Deephaven's native real-time capabilities.
    """
    def __init__(self, colName, scatterFunc, type=None):
        self.output = _Output_(colName, scatterFunc, type)

    def __str__(self):
        print("Column: " + self.output.getColName())
        print("Type: " + self.output.getType())


@_passThrough
def _validate(inputs, outputs, table):

    inputColumns = [input.input.getColNames()[i] for input in inputs for i in range(len(input.input.getColNames()))]

    if not all(inputColumn in list(table.getMeta().getColumn("Name").getDirect()) for inputColumn in inputColumns):
        raise ValueError("All Input columns must exist in the given table.")

    columns = [*set(inputColumns), *[output.output.getColName() for output in outputs]]

    if len(columns) != len(set(columns)):
        raise ValueError("Cannot use existing column names for Output columns.")

    return


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

    _validate(inputs, outputs, table)

    if batch_size == None:
        if table.isLive():
            raise ValueError("Batch size cannot be inferred on a live table. Please specify a batch size.")
        batch_size = table.size()

    globals()["computer"] = _Computer_(table, model_func, [input.input for input in inputs], batch_size)

    if not outputs == None:
        globals()["scatterer"] = _Scatterer_([output.output for output in outputs])

        return table.update("FutureOffset = computer.compute(k)", "Clean = computer.clear()").update(scatterer.generateQueryStrings("FutureOffset")).dropColumns("FutureOffset","Clean")

    return table.update("FutureOffset = computer.compute(k)", "Clean = computer.clear()", "Result = FutureOffset.getFuture().get()").dropColumns("FutureOffset","Clean","Result")

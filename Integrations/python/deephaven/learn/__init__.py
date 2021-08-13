#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""
Deephaven's learn module enables developers and data scientists to use familiar
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

    Raises:
        SystemError : if no JVM has been initialized.
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
        wrapped  : the method to be decorated
        instance : the object to which the wrapped function was bound when it was called
        args     : the argument list for `wrapped`
        kwargs   : the keyword argument dictionary for `wrapped`

    Returns:
        the decorated version of the method.
    """
    _defineSymbols()
    return wrapped(*args, **kwargs)


@_passThrough
class Input:
    """
    Input specifies how to gather data from a Deephaven table into a Python object.
    """
    def __init__(self, colNames, gatherFunc):
        """
        Args:
            colNames   : column name or list of column names from which to gather input.
            gatherFunc : python function that determines how input gets transformed into a python object.
        """
        self.input = _Input_(colNames, gatherFunc)

    def __str__(self):
        return "Columns: " + str([self.input.getColNames()[i] for i in range(len(self.input.getColNames()))])


@_passThrough
class Output:
    """
    Output specifies how to scatter data from a Python object into a table column.
    """
    def __init__(self, colName, scatterFunc, type=None):
        """
        Args:
            colName         : name of the new column that will store results.
            scatterFunc     : python function that determines how data is taken from a python object into a Deephaven table.
            type (optional) : desired data type of the new output column. Must be one of int, long, double, float, boolean, char, String.
        """
        self.output = _Output_(colName, scatterFunc, type)

    def __str__(self):
        print("Column: " + self.output.getColName())
        print("Type: " + self.output.getType())


@_passThrough
def _validate(inputs, outputs, table):
    """
    Ensures that all input columns exist in the table, and that no output column names already exist in the table.

    Args:
        inputs  : list of Inputs to validate.
        outputs : list of Outputs to validate.
        table   : table to check Input and Output columns against.

    Raises:
        ValueError : if at least one of the Input columns does not exist in the table.
        ValueError : if at least one of the Output columns already exists in the table.
    """

    inputColumns = [input.input.getColNames()[i] for input in inputs for i in range(len(input.input.getColNames()))]

    if not all(inputColumn in list(table.getMeta().getColumn("Name").getDirect()) for inputColumn in inputColumns):
        raise ValueError("All Input columns must exist in the given table.")

    if not outputs == None:
        columns = [*set(inputColumns), *[output.output.getColName() for output in outputs]]

        if len(columns) != len(set(columns)):
            raise ValueError("Cannot use existing column names for Output columns.")

    return


@_passThrough
def _verifyColumn(column, table):
    """
    Ensures that the given column is not present in the table, and modifies it if necessary until it is not in the table.

    Args:
        column : column name that should not exist in the table.
        table  : table to check column against.

    Returns:
        the column name, modified if necessary so that it is not present in the table.
    """

    tableColumns = list(table.getMeta().getColumn("Name").getDirect())

    if column not in tableColumns:
        return column

    else:
        i = 0
        while column in tableColumns:
            column = column + str(i)

            if column not in tableColumns:
                return column
            i += 1


@_passThrough
def eval(table=None, model_func=None, inputs=[], outputs=[], batch_size = None):
    """
    Takes relevant data from Deephaven table using inputs, converts that data to the desired Python type, feeds
    it to model_func, and stores that output in a Deephaven table using outputs.

    Args:
        table      : the Deephaven table to perform computations on
        model_func : python function that performs computations on the table.
        inputs     : list of Deephaven input objects
        outputs    : list of Deephaven output objects
        batch_size : number of rows for which model_func is evaluated at once. Note that this must be provided for a live table.

    Returns:
        the table with added columns containing the results of evaluating model_func.

    Raises:
        ValueError : if table is live and no batch size was provided
    """

    _validate(inputs, outputs, table)

    if batch_size == None:
        if table.isLive():
            raise ValueError("Batch size cannot be inferred on a live table. Please specify a batch size.")
        batch_size = table.size()

    globals()["computer"] = _Computer_(table, model_func, [input.input for input in inputs], batch_size)

    futureOffset = _verifyColumn("__FutureOffset", table)
    clean = _verifyColumn("__Clean", table)

    if not outputs == None:
        globals()["scatterer"] = _Scatterer_([output.output for output in outputs])

        return table.update(f"{futureOffset} = computer.compute(k)", f"{clean} = computer.clear()").update(scatterer.generateQueryStrings(f"{futureOffset}")).dropColumns(f"{futureOffset}",f"{clean}")

    result = _verifyColumn("__Result", table)

    return table.update(f"{futureOffset} = computer.compute(k)", f"{clean} = computer.clear()", f"{result} = {futureOffset}.getFuture().get()").dropColumns(f"{futureOffset}",f"{clean}",f"{result}")

#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""
Deephaven's learn module provides utilities for efficient data transfer between Deephaven tables and Python objects,
as well as a framework for using popular machine-learning / deep-learning libraries with Deephaven tables.
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

try:
    _defineSymbols()
except Exception as e:
    pass

@_passThrough
class Input:
    """
    Input specifies how to gather data from a Deephaven table into an object.
    """

    def __init__(self, colNames, gatherFunc):
        """
        Initializes an Input object with the given arguments.

        Args:
            colNames   : column name or list of column names from which to gather input.
            gatherFunc : function that determines how input gets transformed into an object.
        """
        self.input = _Input_(colNames, gatherFunc)

    def __str__(self):
        """
        Returns the Input object as a string.

        Returns:
            Input as a string.
        """
        return self.input.toString()


@_passThrough
class Output:
    """
    Output specifies how to scatter data from an object into a table column.
    """

    def __init__(self, colName, scatterFunc, type=None):
        """
        Initializes an Output object with the given arguments.

        Args:
            colName         : name of the new column that will store results.
            scatterFunc     : function that determines how data is taken from an object and placed into a Deephaven table column.
            type (optional) : desired data type of the new output column.
        """
        self.output = _Output_(colName, scatterFunc, type)

    def __str__(self):
        """
        Returns the Output object as a string.

        Returns:
            Output as a string.
        """
        return self.output.toString()


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
        ValueError : if there are duplicates in the Output column names.
    """

    if type(inputs) != list:
        inputs = [inputs]

    inputColumns = [input.input.getColNames()[i] for input in inputs for i in range(len(input.input.getColNames()))]

    if table.hasColumns(inputColumns):

        if outputs != None:

            if type(outputs) != list:
                outputs = [outputs]

            outputColumns = [output.output.getColName() for output in outputs]

            if len(outputColumns) != len(set(outputColumns)):
                repeats = set([column for column in outputColumns if outputColumns.count(column) > 1])
                raise ValueError(f"Cannot assign the same column name {repeats} to multiple columns.")

            elif table.hasColumns(outputColumns):
                overlap = set(outputColumns).intersection(table.getMeta().getColumn("Name").getDirect())
                raise ValueError(f"The columns {overlap} already exist in the table. Please choose Output column names that are not already in the table.")

            else:
                return

        return

    else:
        difference = set(inputColumns).difference(table.getMeta().getColumn("Name").getDirect())
        raise ValueError(f"Cannot find columns {difference} in the table.")


@_passThrough
def _createNonconflictingColumnName(table, baseColumnName):
    """
    Creates a column name that is not present in the table.

    Args:
        table          : table to check column name against.
        baseColumnName : base name to create a column from.

    Returns:
        column name that is not present in the table.
    """

    if not table.hasColumns(baseColumnName):
        return baseColumnName

    else:
        i = 0
        while table.hasColumns(baseColumnName):
            baseColumnName = baseColumnName + str(i)

            if not table.hasColumns(baseColumnName):
                return baseColumnName
            i += 1


@_passThrough
def learn(table=None, model_func=None, inputs=[], outputs=[], batch_size = None):
    """
    Learn gathers data from multiple rows of the input table, performs a calculation, and scatters values from the
    calculation into an output table. This is a common computing paradigm for artificial intelligence, machine learning,
    and deep learning.

    Args:
        table      : the Deephaven table to perform computations on.
        model_func : function that performs computations on the table.
        inputs     : list of Input objects that determine how data gets extracted from the table.
        outputs    : list of Output objects that determine how data gets scattered back into the results table.
        batch_size : maximum number of rows for which model_func is evaluated at once.

    Returns:
        the table with added columns containing the results of evaluating model_func.

    Raises:
        ValueError : if no batch size was provided.
        ValueError : if at least one of the Input columns does not exist in the table.
        ValueError : if at least one of the Output columns already exists in the table.
        ValueError : if there are duplicates in the Output column names.
    """

    _validate(inputs, outputs, table)

    if batch_size == None:
        raise ValueError("Batch size cannot be inferred. Please specify a batch size.")

    #TODO: When ticket #1072 is resolved, the following code should be replaced with
    # Globals["__computer"] = _Computer_(table, model_func, [input.input for input in inputs], batch_size)
    # and remove from globals at the end of function
    jpy.get_type("io.deephaven.engine.table.lang.QueryScope").addParam("__computer", _Computer_(table, model_func, [input.input for input in inputs], batch_size))

    futureOffset = _createNonconflictingColumnName(table, "__FutureOffset")
    clean = _createNonconflictingColumnName(table, "__CleanComputer")

    if not outputs == None:
        __scatterer = _Scatterer_([output.output for output in outputs])
        #TODO: Similarly at resolution of #1072, replace the following code with
        # Globals["__scatterer"] = __scatterer
        # and remove from Globals at end of function
        jpy.get_type("io.deephaven.engine.table.lang.QueryScope").addParam("__scatterer", __scatterer)

        return table.update(f"{futureOffset} = __computer.compute(k)")\
                .update(__scatterer.generateQueryStrings(f"{futureOffset}"))\
                .update(f"{clean} = __computer.clear()")\
                .dropColumns(f"{futureOffset}", f"{clean}")

    result = _createNonconflictingColumnName(table, "__Result")

    return table.update(f"{futureOffset} = __computer.compute(k)", f"{result} = {futureOffset}.getFuture().get()", f"{clean} = __computer.clear()")\
            .dropColumns(f"{futureOffset}", f"{clean}", f"{result}")
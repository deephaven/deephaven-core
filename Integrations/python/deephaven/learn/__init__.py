#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""
Deephaven's learn module enables developers and data scientists to use familiar Python frameworks in tandem
with Deephaven's real-time data tech for a powerful, flexible, live AI/ML library.
"""

import jpy
import wrapt

_Input_ = None
_Output_ = None
_Computer_ = None
_Scatterer_ = None
_QueryScope_ = None


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

    global _Input_, _Output_, _Computer_, _Scatterer_, _QueryScope_
    if _Input_ is None:
        _Input_ = jpy.get_type("io.deephaven.integrations.learn.Input")
        _Output_ = jpy.get_type("io.deephaven.integrations.learn.Output")
        _Computer_ = jpy.get_type("io.deephaven.integrations.learn.Computer")
        _Scatterer_ = jpy.get_type("io.deephaven.integrations.learn.Scatterer")
        _QueryScope_ = jpy.get_type("io.deephaven.db.tables.select.QueryScope")


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
            scatterFunc     : function that determines how data is taken from a python object into a Deephaven table.
            type (optional) : desired data type of the new output column.
        """
        self.output = _Output_(colName, scatterFunc, type)

    def __str__(self):
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
    """

    inputColumns = [input.input.getColNames()[i] for input in inputs for i in range(len(input.input.getColNames()))]

    if table.hasColumns(inputColumns):

        if not outputs == None:
            outputColumns = [output.output.getColName() for output in outputs]

            if not table.hasColumns(outputColumns):
                return

            else:
                overlap = set(outputColumns).intersection(table.getMeta().getColumn("Name").getDirect())
                raise ValueError(f"The columns {overlap} already exist in the table. Please choose Output column names that are not already in the table.")

        return

    else:
        difference = set(inputColumns).difference(table.getMeta().getColumn("Name").getDirect())
        raise ValueError(f"Cannot find columns {difference} in the table. Please check your spelling and try again.")


@_passThrough
def _createNonconflictingColumnName(column, table):
    """
    Ensures that the given column is not present in the table, and modifies it if necessary until it is not in the table.

    Args:
        column : column name that should not exist in the table.
        table  : table to check column against.

    Returns:
        the column name, modified if necessary so that it is not present in the table.
    """

    if not table.hasColumns(column):
        return column

    else:
        i = 0
        while table.hasColumns(column):
            column = column + str(i)

            if not table.hasColumns(column):
                return column
            i += 1


@_passThrough
def exec(table=None, model_func=None, inputs=[], outputs=[], batch_size = None):
    """
    This is the primary tool for linking Deephaven tables with Python deep learning libraries.

    The inputs are used to collect data from the table; then, this data is passed through the model_func for computing,
    and the results are scattered back into the table with the outputs.

    Here, 

    Args:
        table      : the Deephaven table to perform computations on.
        model_func : function that performs computations on the table.
        inputs     : list of Input objects that determine how data gets extracted from the table.
        outputs    : list of Output objects that determine how data gets collected back into the table.
        batch_size : number of rows for which model_func is evaluated at once. Note that this must be provided for a live table.

    Returns:
        the table with added columns containing the results of evaluating model_func.

    Raises:
        ValueError : if table is live and no batch size was provided.
        ValueError : if at least one of the Input columns does not exist in the table.
        ValueError : if at least one of the Output columns already exists in the table.
    """

    _validate(inputs, outputs, table)

    if batch_size == None:
        if table.isLive():
            raise ValueError("Batch size cannot be inferred on a live table. Please specify a batch size.")
        batch_size = table.size()

    #TODO: When ticket #1072 is resolved, the following code should be replaced with
    # Globals["__computer"] = _Computer_(table, model_func, [input.input for input in inputs], batch_size)
    # and remove from globals at the end of function
    _QueryScope_.addParam("__computer", _Computer_(table, model_func, [input.input for input in inputs], batch_size))

    futureOffset = _createNonconflictingColumnName("__FutureOffset", table)
    clean = _createNonconflictingColumnName("__Clean", table)

    if not outputs == None:
        __scatterer = _Scatterer_([output.output for output in outputs])
        #TODO: Similarly at resolution of #1072, replace the following code with
        # Globals["__scatterer"] = __scatterer
        # and remove from Globals at end of function
        _QueryScope_.addParam("__scatterer", __scatterer)

        return table.update(f"{futureOffset} = __computer.compute(k)", f"{clean} = __computer.clear()").update(__scatterer.generateQueryStrings(f"{futureOffset}")).dropColumns(f"{futureOffset}", f"{clean}")

    result = _createNonconflictingColumnName("__Result", table)

    return table.update(f"{futureOffset} = __computer.compute(k)", f"{clean} = __computer.clear()", f"{result} = {futureOffset}.getFuture().get()").dropColumns(f"{futureOffset}", f"{clean}", f"{result}")
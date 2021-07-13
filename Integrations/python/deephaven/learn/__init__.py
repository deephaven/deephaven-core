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
import numpy as np
from deephaven import npy
from deephaven import listen
from deephaven import QueryScope

_class_name_ = "io.deephaven.integrations.numpy.Java2NumpyCopy"
_Java2NumpyCopy_ = None


# the following two methods will only be used once I implement some parts of this code in java. They are here now for
# completeness but will not be used until I need to invoke java methods.
def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _Java2NumpyCopy_
    if _Java2NumpyCopy_ is None:
        # This will raise an exception if the desired object is not in the classpath
        _Java2NumpyCopy_ = jpy.get_type(_class_name_)


# every module method should be decorated with @_passThrough
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
    The Input class provides an interface for converting Deephaven tables to objects that Python deep learning libraries
    are familiar with. Input objects are intended to be used as the input argument of an eval() function call.
    """
    def __init__(self, columns, gather):
        """
        :param columns: the list of column names from a Deephaven table that you want to use in modelling
        :param gather: the function that determines how data from a Deephaven table is collected
        """
        if type(columns) is list:
            self.columns = columns
        else:
            self.columns = [columns]

        self.gather = gather


class Output:
    """
    The Output class provides an interface for converting Python objects (such as tensors or dataframes) into Deephaven
    tables. Output objects are intended to be used as the output argument of an eval() function call.
    """
    def __init__(self, column, scatter, col_type="java.lang.Object"):
        """
        :param column: the string name of the column you would like to create to store your output
        :param scatter: the function that determines how data from a Python object is stored in a Deephaven table
        :param col_type: optional string that defaults to 'java.lang.Object', determines the type of output column
        """
        self.column = column
        self.scatter = scatter
        self.col_type = col_type


#TODO: this should be implemented in Java for speed.  This efficiently iterates over the indices in multiple index sets.  Works for hist and real time.
class _IndexSetIterator:
    """
    The IndexSetIterator class provides functionality for iterating over multiple sets of given indices for static or real
    time tables. IndexSetIterator objects are used to subset tables by indices that have been added or modified, in the
    case of a real time table.
    """
    def __init__(self, *indexes):
        """
        :param indexes: the set or multiple sets of indices that determine how to subset a Deephaven table
        """
        self.indexes = indexes

    def __len__(self):
        rst = 0

        for index in self.indexes:
            rst += index.size()

        return rst

    def __iter__(self):
        for index in self.indexes:
            it = index.iterator()

            while it.hasNext():
                yield it.next()


class _ListenAndReturn:
    def __init__(self, table, model_func, inputs, outputs, col_sets):
        self.table = table
        self.model_func = model_func
        self.inputs = inputs
        self.outputs = outputs
        self.col_sets = col_sets
        self.newTable = None

    def onUpdate(self, isReplay, update):
        self.idx = _IndexSetIterator(update.added, update.modified)
        self.gathered = [ input.gather(self.idx, col_set) for (input,col_set) in zip(self.inputs, self.col_sets) ]
        self.newTable = _create_output(self.table, self.model_func, self.gathered, self.outputs)


# I think this is the place to handle errors for input, should make sure multiple input column sets are mutually exclusive
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
        # if list of features is empty, replace with all columns and return
        if len(inputs[0].columns) == 0:
            new_inputs[0].columns = list(table.getMeta().getColumn("Name").getDirect())
            return new_inputs
        else:
            return new_inputs
    else:
        # now that we know input length at least 2, ensure target non-empty
        if len(inputs[0].columns) == 0:
            raise ValueError('Target input cannot be empty.')
        else:
            target = inputs[0].columns
            # look through every other input to find empty list
            for i in range(1,len(inputs)):
                if len(inputs[i].columns) == 0:
                    new_inputs[i].columns = list(table.dropColumns(target).getMeta().getColumn("Name").getDirect())
                else:
                    pass
            return new_inputs


# TODO: at the moment this cannot handle real time, real time can be kind of accomplished by uncommenting 198 amd 203
def _create_output(table=None, model_func=None, gathered=[], outputs=[]):
    """
    Passes gathered inputs to model_func to create output, then uses the list of Output objects to store the newly
    created output in the given Deephaven table.

    :param table: the Deephaven used for computing and storing output
    :param model_func: the user-defined function for performing computations on the dataset
    :param gathered: the list of Python objects that results from applying the gather function to the Deephaven table
    :param outputs: the list of Output objects that determine how data will be stored in the Deephaven table
    """
    # if there are no outputs, we just want to call model_func and return nothing
    if outputs == None:
        print("COMPUTE NEW DATA")
        model_func(*gathered)
        return

    else:
        print("COMPUTE NEW DATA")
        output_values = model_func(*gathered)
        #print(output_values)

        print("POPULATE OUTPUT TABLE")
        rst = table.by()

        #return

        n = table.size()

        for output in outputs:
            print(f"GENERATING OUTPUT: {output.column}")
            #TODO: maybe we can infer the type
            data = jpy.array(output.col_type, n)

            #TODO: python looping is slow.  should avoid or numba it
            # this is the line that breaks, the bad logic is probably elsewhere
            for i in range(n):
                data[i] = output.scatter(output_values, i)

            QueryScope.addParam("__temp", data)
            rst = rst.update(f"{output.column} = __temp")

        return rst.ungroup()


def eval(table=None, model_func=None, live=False, inputs=[], outputs=[]):
    """
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
    print("SETUP")
    inputs = _parse_input(inputs, table)
    col_sets = [ [ table.getColumnSource(col) for col in input.columns ] for input in inputs ]

    print("GATHER")
    # this is where we need to begin making the distinction between static and live data
    if live:
        # instantiate class to listen to updates and update output accordingly
        listener = _ListenAndReturn(table, model_func, inputs, outputs, col_sets)
        handle = listen(table, listener, replay_initial=True)

    else:
        idx = _IndexSetIterator(table.getIndex())
        gathered = [ input.gather(idx, col_set) for (input,col_set) in zip(inputs,col_sets) ]
        return _create_output(table, model_func, gathered, outputs)

########################################################################################################################
# Finally, we should provide some common gather and scatter functions
########################################################################################################################

import jpy
import numpy as np
from deephaven import npy


_java_type_ = None  # None until the first _defineSymbols() call


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.integrations.numpy.Java2NumpyCopy")


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


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass


def _perform_split(table, feature, response, weight):
    #pseudocode
    """
    first, compute number of training rows n with ceil(table.size() * weight)
    then, use _java_type_.randRows(n, table.size(), replace=False)
        this will yield an array of row indices to identify training data

    magic: use indices to subset Deephaven table without going through numpy

    finally, check if feature and response are both None. If so, use entire table
    if feature is None and response is given, response = response, feature = all but response
    if feature is given and response is None, no response, feature = feature
    if both feature and response are given, feature = feature, response = response

    return (java.util.List<io.deephaven.db.tables.Table>) - a List of Deephaven table objects
    """

    # verify that weight is a valid value
    if (not isinstance(weight, (float, double))) or (weight <= 0 or weight >= 1):
        raise ValueError("weight must be a number between 0 and 1 (exclusive).")

    # gives number of rows to select based on table size and weight
    nRow = int(np.ceil(table.size() * weight))
    # calls Java2NumnpyCopy's randRows method to randomly select rows without replacement
    rows = _java_type_.randRows(nRow, table.size())

    return rows


@_passThrough
def train_test_split(table, feature=None, response=None, weight=.8):

    """
    Performs a row-wise split on the given data.

    :param table: (io.deephaven.db.tables.Table) - Deephaven table object to split
    :param feature: (java.util.Collection<java.lang.String>) - column names to use as features, defaults to none
        to get all but response
    :param response: (java.lang.String) - the column to use for response in supervised learning
    :return (java.util.List<io.deephaven.db.tables.Table>) - a list of Deephaven table objects

    here, need to unpack into python list and return
    """

    #split_tables = _perform_split(table, feature, response, weight)

    return _perform_split(table, feature, response, weight)

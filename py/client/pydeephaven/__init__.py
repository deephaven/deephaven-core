#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

"""Deephaven Python Client (`pydeephaven`) is a Python API built on top of Deephaven's highly efficient Open API which is
based on gRPC and Apache Arrow. It allows Python applications to remotely connect to Deephaven data servers,
export/import data with the server, run Python scripts on the server, and execute powerful queries on data tables.

Because Deephaven data servers and Deephaven clients including pydeephaven exchange data in the Apache Arrow format,
pydeephaven is able to leverage 'pyarrow' - the Python bindings of Arrow (https://arrow.apache.org/docs/python/) for
data representation and integration with other data analytic tools such as NumPy, Pandas, etc.


Examples:
    >>> from pydeephaven import Session
    >>> from pyarrow import csv
    >>> session = Session() # assuming Deephaven Community Edition is running locally with the default configuration
    >>> table1 = session.import_table(csv.read_csv("data1.csv"))
    >>> table2 = session.import_table(csv.read_csv("data2.csv"))
    >>> joined_table = table1.join(table2, on=["key_col_1", "key_col_2"], joins=["data_col1"])
    >>> df = joined_table.to_arrow().to_pandas()
    >>> print(df)
    >>> session.close()
"""

import importlib.metadata

from .session import Session
from .dherror import DHError
from ._table_interface import SortDirection
from .query import Query
from .table import Table

try:
    from pydeephaven_ticking.table_listener import TableListener, TableListenerHandle, TableUpdate, listen
except ImportError:
    pass

__all__ = ["Session", "DHError", "SortDirection"]

# Note: this is the _distribution_ name, not the _package_ name. Until 3.10, there is not an easy way to get the
# distribution name from the package name.
# https://docs.python.org/3/library/importlib.metadata.html#package-distributions
__version__ = importlib.metadata.version('pydeephaven')

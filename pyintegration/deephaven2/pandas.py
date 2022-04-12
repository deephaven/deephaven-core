#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module supports the conversion between Deephaven tables and Pandas DataFrames. """
import re
from typing import List

import jpy
import numpy as np
import pandas

from deephaven2 import DHError, new_table, dtypes
from deephaven2.column import Column
from deephaven2.numpy import _column_to_numpy_array, freeze_table, _make_input_column
from deephaven2.table import Table

_JPrimitiveArrayConversionUtility = jpy.get_type("io.deephaven.integrations.common.PrimitiveArrayConversionUtility")


def _column_to_series(table: Table, col_def: Column) -> pandas.Series:
    """ Produce a copy of the specified column as a pandas.Series object.

    Args:
        table (Table): the table
        col_def (Column):  the column definition

    Returns:
        pandas.Series

    Raises:
        DHError
    """
    try:
        data_col = table.j_table.getColumn(col_def.name)
        np_array = _column_to_numpy_array(col_def, data_col.getDirect())

        return pandas.Series(data=np_array, copy=False)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, message="failed to create apandas Series for {col}") from e


def to_pandas(table: Table, cols: List[str] = None) -> pandas.DataFrame:
    """  Produces a pandas.DataFrame from a table.

    Note that the **entire table** is going to be cloned into memory, so the total number of entries in the table
    should be considered before blindly doing this. For large tables, consider using the Deephaven query language to
    select a subset of the table **before** using this method.

    Args:
        table (Table): the source table
        cols (List[str]): the source column names, default is None which means include all columns

    Returns:
        pandas.DataFrame

    Raise:
        DHError
    """
    try:
        if table.is_refreshing:
            table = freeze_table(table)

        col_def_dict = {col.name: col for col in table.columns}
        if not cols:
            cols = list(col_def_dict.keys())
        else:
            diff_set = set(cols) - set(col_def_dict.keys())
            if diff_set:
                raise DHError(message=f"columns - {list(diff_set)} not found")

        data = {}
        for col in cols:
            series = _column_to_series(table, col_def_dict[col])
            data[col] = series

        dtype_set = set([v.dtype for k, v in data.items()])
        if len(dtype_set) == 1:
            return pandas.DataFrame(data=np.stack([v.array for k, v in data.items()], axis=1),
                                    columns=cols,
                                    copy=False)
        else:
            return pandas.DataFrame(data=data, columns=cols, copy=False)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, "failed to create a Pandas DataFrame from table.") from e


def to_table(df: pandas.DataFrame, cols: List[str] = None) -> Table:
    """  Creates a new table from a pandas.DataFrame.

    Args:
        df (DataFrame): the Pandas DataFrame instance
        cols (List[str]): the dataframe column names, default is None which means including all columns in the dataframe

    Returns:
        a Deephaven table

    Raise:
        DHError
    """

    try:
        if not cols:
            cols = list(df)
        else:
            diff_set = set(cols) - set(list(df))
            if diff_set:
                raise DHError(message=f"columns - {list(diff_set)} not found")

        input_cols = []
        for col in cols:
            input_cols.append(_make_input_column(col, df.get(col).values))

        return new_table(cols=input_cols)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, "failed to create a Deephaven Table from a Pandas DataFrame.") from e

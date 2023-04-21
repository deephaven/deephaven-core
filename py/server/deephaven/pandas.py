#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module supports the conversion between Deephaven tables and Pandas DataFrames. """
from typing import List

import jpy
import numpy as np
import pandas
import pandas as pd
import pyarrow as pa

from deephaven import DHError, new_table, dtypes, arrow
from deephaven.arrow import SUPPORTED_ARROW_TYPES
from deephaven.column import Column
from deephaven.constants import NULL_BYTE, NULL_SHORT, NULL_INT, NULL_LONG, NULL_FLOAT, NULL_DOUBLE
from deephaven.numpy import column_to_numpy_array, _make_input_column
from deephaven.table import Table

_JPrimitiveArrayConversionUtility = jpy.get_type("io.deephaven.integrations.common.PrimitiveArrayConversionUtility")


def _column_to_series(table: Table, col_def: Column) -> pandas.Series:
    """Produce a copy of the specified column as a pandas.Series object.

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
        np_array = column_to_numpy_array(col_def, data_col.getDirect())

        return pandas.Series(data=np_array, copy=False)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, message="failed to create apandas Series for {col}") from e


_DTYPE_MAPPING_PYARROW = {
    pa.int8(): pd.ArrowDtype(pa.int8()),
    pa.int16(): pd.ArrowDtype(pa.int16()),
    pa.int32(): pd.ArrowDtype(pa.int32()),
    pa.int64(): pd.ArrowDtype(pa.int64()),
    pa.uint8(): pd.ArrowDtype(pa.uint16()),
    pa.uint16(): pd.ArrowDtype(pa.uint64()),
    pa.uint32(): pd.ArrowDtype(pa.uint32()),
    pa.uint64(): pd.ArrowDtype(pa.uint64()),
    pa.bool_(): pd.ArrowDtype(pa.bool_()),
    pa.float32(): pd.ArrowDtype(pa.float32()),
    pa.float64(): pd.ArrowDtype(pa.float64()),
    pa.string(): pd.ArrowDtype(pa.string()),
    pa.timestamp('ns'): pd.ArrowDtype(pa.timestamp('ns')),
    pa.timestamp('ns', tz='UTC'): pd.ArrowDtype(pa.timestamp('ns', tz='UTC')),
}

_DTYPE_MAPPING_NUMPY_NULLABLE = {
    pa.int8(): pd.Int8Dtype(),
    pa.int16(): pd.Int16Dtype(),
    pa.int32(): pd.Int32Dtype(),
    pa.int64(): pd.Int64Dtype(),
    pa.bool_(): pd.BooleanDtype(),
    pa.float32(): pd.Float32Dtype(),
    pa.float64(): pd.Float64Dtype(),
    pa.string(): pd.StringDtype(),
    # pa.Table.to_pandas() doesn't support mapping to pd.DatetimeTZDtype,
    # pa.timestamp('ns'): pd.DatetimeTZDtype(unit='ns', tz='UTC'),
    # pa.timestamp('ns', tz='UTC'): pd.DatetimeTZDtype(unit='ns', tz='UTC'),
}

_TO_PANDAS_TYPE_MAPPERS = {
    "pyarrow": _DTYPE_MAPPING_PYARROW.get,
    "numpy_nullable": _DTYPE_MAPPING_NUMPY_NULLABLE.get,
}


def to_pandas(table: Table, cols: List[str] = None, dtype_backend: str = None) -> pandas.DataFrame:
    """Produces a pandas.DataFrame from a table.

    Note that the **entire table** is going to be cloned into memory, so the total number of entries in the table
    should be considered before blindly doing this. For large tables, consider using the Deephaven query language to
    select a subset of the table **before** using this method.

    Args:
        table (Table): the source table
        cols (List[str]): the source column names, default is None which means include all columns
        dtype_backend (str): Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
            nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set,
            pyarrow is used for all dtypes if “pyarrow” is set. default is None, meaning Numpy backed DataFrames with
            no nullable dtypes.

    Returns:
        pandas.DataFrame

    Raise:
        DHError
    """
    try:
        if dtype_backend is not None and pandas.__version__.partition(".")[0] == "1":
            raise DHError(message="the dtype_backend option is only available for Pandas 2.0.0 and above.")

        type_mapper = _TO_PANDAS_TYPE_MAPPERS.get(dtype_backend)
        # if nullable dtypes (Pandas or pyarrow) is requested
        if type_mapper:
            pa_table = arrow.to_arrow(table=table, cols=cols)
            df = pa_table.to_pandas(types_mapper=type_mapper)
            del pa_table
            return df

        # if regular numpy dtype is request, we will directly access the table column sources, to get a consistent
        # view of a ticking table, we need to take a snapshot of it first
        if table.is_refreshing:
            table = table.snapshot()

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


_EX_DTYPE_NULL_MAP = {
    # This reflects the fact that in the server we use NULL_BOOLEAN_AS_BYTE - the byte encoding of null boolean to
    # translate boxed Boolean to/from primitive bytes
    pd.BooleanDtype: NULL_BYTE,
    pd.Int8Dtype: NULL_BYTE,
    pd.Int16Dtype: NULL_SHORT,
    pd.Int32Dtype: NULL_INT,
    pd.Int64Dtype: NULL_LONG,
    pd.Float32Dtype: NULL_FLOAT,
    pd.Float64Dtype: NULL_DOUBLE,
    pd.StringDtype: None,
}


def _map_na(np_array: np.ndarray):
    """Replaces the pd.NA values in the array if it is of Pandas ExtensionDtype(nullable)."""
    pd_dtype = np_array.dtype
    if not isinstance(pd_dtype, pandas.api.extensions.ExtensionDtype):
        return np_array

    dh_null = _EX_DTYPE_NULL_MAP.get(type(pd_dtype), None)
    if isinstance(pd_dtype, pd.StringDtype) or isinstance(pd_dtype, pd.BooleanDtype):
        np_array = np.array(list(map(lambda v: dh_null if v is pd.NA else v, np_array)))
    elif dh_null is not None:
        np_array = np_array.fillna(dh_null)

    return np_array


def _is_pyarrow_backed(dtype) -> bool:
    """Checks if a Pandas dtype is a pyarrow type."""
    if isinstance(dtype, pd.ArrowDtype) and dtype.pyarrow_dtype in SUPPORTED_ARROW_TYPES:
        return True
    if isinstance(dtype, pandas.api.extensions.ExtensionDtype) and hasattr(dtype, "storage") \
            and dtype.storage == "pyarrow":
        return True

    return False


def to_table(df: pandas.DataFrame, cols: List[str] = None) -> Table:
    """Creates a new table from a pandas.DataFrame.

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

        # check if all the cols are pyarrow-backed, and if so, create an intermediate pyarrow table to upload to DH
        df_dtypes = [df[col].dtype for col in cols]
        if all(_is_pyarrow_backed(dt) for dt in df_dtypes):
            pa_arrays = []
            for col in cols:
                pa_arrays.append(df[col].array)
            pa_table = pa.Table.from_arrays(arrays=pa_arrays, names=cols)
            dh_table = arrow.to_table(pa_table)
            del pa_table
            return dh_table

        if any(_is_pyarrow_backed(dt) for dt in df_dtypes):
            raise DHError(message="to_table doesn't support a mixture of pyarrow-backed arrays and numpy arrays")

        input_cols = []
        for col in cols:
            np_array = df.get(col).values
            dtype = dtypes.from_np_dtype(np_array.dtype)
            np_array = _map_na(np_array)
            input_cols.append(_make_input_column(col, np_array, dtype))

        return new_table(cols=input_cols)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, "failed to create a Deephaven Table from a Pandas DataFrame.") from e

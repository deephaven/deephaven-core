#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module supports the conversion between Deephaven tables and pandas DataFrames. """
from typing import List

import jpy
import numpy as np
import pandas as pd
import pyarrow as pa

from deephaven import DHError, new_table, dtypes, arrow
from deephaven.arrow import SUPPORTED_ARROW_TYPES
from deephaven.column import Column
from deephaven.constants import NULL_BYTE, NULL_SHORT, NULL_INT, NULL_LONG, NULL_FLOAT, NULL_DOUBLE
from deephaven.numpy import column_to_numpy_array, _make_input_column
from deephaven.table import Table

_JPrimitiveArrayConversionUtility = jpy.get_type("io.deephaven.integrations.common.PrimitiveArrayConversionUtility")
_is_dtype_backend_supported = pd.__version__ >= "2.0.0"


def _column_to_series(table: Table, col_def: Column) -> pd.Series:
    """Produce a copy of the specified column as a pandas.Series object.

    Args:
        table (Table): the table
        col_def (Column):  the column definition

    Returns:
        a pandas Series

    Raises:
        DHError
    """
    try:
        data_col = table.j_table.getColumn(col_def.name)
        np_array = column_to_numpy_array(col_def, data_col.getDirect())

        return pd.Series(data=np_array, copy=False)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, message="failed to create a pandas Series for {col}") from e


_DTYPE_MAPPING_PYARROW = {
    pa.int8(): pd.ArrowDtype(pa.int8()),
    pa.int16(): pd.ArrowDtype(pa.int16()),
    pa.int32(): pd.ArrowDtype(pa.int32()),
    pa.int64(): pd.ArrowDtype(pa.int64()),
    pa.uint8(): pd.ArrowDtype(pa.uint8()),
    pa.uint16(): pd.ArrowDtype(pa.uint16()),
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
    # pa.Table.to_pandas() doesn't like explicit mapping to pd.DatetimeTZDtype, however it, on its own,
    # can correctly map pyarrow timestamp to DatetimeTZDtype and convert null values to NaT
    # pa.timestamp('ns'): pd.DatetimeTZDtype(unit='ns', tz='UTC'),
    # pa.timestamp('ns', tz='UTC'): pd.DatetimeTZDtype(unit='ns', tz='UTC'),
}

_PYARROW_TO_PANDAS_TYPE_MAPPERS = {
    "pyarrow": _DTYPE_MAPPING_PYARROW.get,
    "numpy_nullable": _DTYPE_MAPPING_NUMPY_NULLABLE.get,
}


def to_pandas(table: Table, cols: List[str] = None, dtype_backend: str = None) -> pd.DataFrame:
    """Produces a pandas DataFrame from a table.

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
        a pandas DataFrame

    Raise:
        DHError
    """
    try:
        if dtype_backend is not None and not _is_dtype_backend_supported:
            raise DHError(message=f"the dtype_backend ({dtype_backend}) option is only available for pandas 2.0.0 and "
                                  f"above. {pd.__version__} is being used.")

        type_mapper = _PYARROW_TO_PANDAS_TYPE_MAPPERS.get(dtype_backend)
        # if nullable dtypes (pandas or pyarrow) is requested
        if type_mapper:
            pa_table = arrow.to_arrow(table=table, cols=cols)
            df = pa_table.to_pandas(types_mapper=type_mapper)
            del pa_table
            return df

        # if regular numpy dtype is requested, direct access of the table column sources is required. In order to get a
        # consistent view of a ticking table, we need to take a snapshot of it first.
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
            return pd.DataFrame(data=np.stack([v.array for k, v in data.items()], axis=1),
                                columns=cols,
                                copy=False)
        else:
            return pd.DataFrame(data=data, columns=cols, copy=False)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, "failed to create a pandas DataFrame from table.") from e


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
    pd.ArrowDtype(pa.int8()): NULL_BYTE,
    pd.ArrowDtype(pa.int16()): NULL_SHORT,
    pd.ArrowDtype(pa.int32()): NULL_INT,
    pd.ArrowDtype(pa.int64()): NULL_LONG,
    pd.ArrowDtype(pa.bool_()): NULL_BYTE,
    pd.ArrowDtype(pa.float32()): NULL_FLOAT,
    pd.ArrowDtype(pa.float64()): NULL_DOUBLE,
    pd.ArrowDtype(pa.string()): None,
}


def _map_na(np_array: np.ndarray):
    """Replaces the pd.NA values in the array if it is of pandas ExtensionDtype(nullable)."""
    pd_dtype = np_array.dtype
    if not isinstance(pd_dtype, pd.api.extensions.ExtensionDtype):
        return np_array

    dh_null = _EX_DTYPE_NULL_MAP.get(type(pd_dtype)) or _EX_DTYPE_NULL_MAP.get(pd_dtype)
    if isinstance(pd_dtype, pd.StringDtype) or isinstance(pd_dtype, pd.BooleanDtype) or pd_dtype == pd.ArrowDtype(
            pa.bool_()):
        np_array = np.array(list(map(lambda v: dh_null if v is pd.NA else v, np_array)))
    elif dh_null is not None:
        np_array = np_array.fillna(dh_null)

    return np_array


def to_table(df: pd.DataFrame, cols: List[str] = None) -> Table:
    """Creates a new table from a pandas DataFrame.

    Args:
        df (DataFrame): the pandas DataFrame instance
        cols (List[str]): the dataframe column names, default is None which means including all columns in the DataFrame

    Returns:
        a Deephaven table

    Raise:
        DHError
    """

    if not cols:
        cols = list(df)
    else:
        diff_set = set(cols) - set(list(df))
        if diff_set:
            raise DHError(message=f"columns - {list(diff_set)} not found")

    # if any arrow backed column is present, create a pyarrow table first, then upload to DH, if error occurs, fall
    # back to the numpy-array based approach
    if _is_dtype_backend_supported and any(isinstance(df[col].dtype, pd.ArrowDtype) for col in cols):
        try:
            pa_table = pa.Table.from_pandas(df=df, columns=cols)
            dh_table = arrow.to_table(pa_table)
            return dh_table
        except:
            pass

    try:
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
        raise DHError(e, "failed to create a Deephaven Table from a pandas DataFrame.") from e

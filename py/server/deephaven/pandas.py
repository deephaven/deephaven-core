#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module supports the conversion between Deephaven tables and pandas DataFrames. """
from typing import List, Dict, Tuple

import jpy
import numpy as np
import pandas as pd
import pyarrow as pa

from deephaven import DHError, new_table, dtypes, arrow
from deephaven.column import Column
from deephaven.constants import NULL_BYTE, NULL_SHORT, NULL_INT, NULL_LONG, NULL_FLOAT, NULL_DOUBLE, NULL_CHAR
from deephaven.dtypes import DType
from deephaven.numpy import column_to_numpy_array, _make_input_column
from deephaven.table import Table

_NULL_BOOLEAN_AS_BYTE = jpy.get_type("io.deephaven.util.BooleanUtils").NULL_BOOLEAN_AS_BYTE
_JPrimitiveArrayConversionUtility = jpy.get_type("io.deephaven.integrations.common.PrimitiveArrayConversionUtility")
_JDataAccessHelpers = jpy.get_type("io.deephaven.engine.table.impl.DataAccessHelpers")
_is_dtype_backend_supported = pd.__version__ >= "2.0.0"

_DTYPE_NULL_MAPPING: Dict[DType, Tuple] = {
    dtypes.bool_: (_NULL_BOOLEAN_AS_BYTE, pd.BooleanDtype),
    dtypes.byte: (NULL_BYTE, pd.Int8Dtype),
    dtypes.short: (NULL_SHORT, pd.Int16Dtype),
    dtypes.char: (NULL_CHAR, pd.UInt16Dtype),
    dtypes.int32: (NULL_INT, pd.Int32Dtype),
    dtypes.int64: (NULL_LONG, pd.Int64Dtype),
    dtypes.float32: (NULL_FLOAT, pd.Float32Dtype),
    dtypes.float64: (NULL_DOUBLE, pd.Float64Dtype),
}


def _column_to_series(table: Table, col_def: Column, conv_null: bool) -> pd.Series:
    """Produce a copy of the specified column as a pandas.Series object.

    Args:
        table (Table): the table
        col_def (Column):  the column definition
        conv_null (bool): whether to check for Deephaven nulls in the data and automatically replace them with
            pd.NA.

    Returns:
        a pandas Series

    Raises:
        DHError
    """
    try:
        data_col = _JDataAccessHelpers.getColumn(table.j_table, col_def.name)
        if conv_null and col_def.data_type == dtypes.bool_:
            j_array = _JPrimitiveArrayConversionUtility.translateArrayBooleanToByte(data_col.getDirect())
            np_array = np.frombuffer(j_array, dtype=np.byte)
            s = pd.Series(data=np_array, dtype=pd.Int8Dtype(), copy=False)
            s.mask(s == _NULL_BOOLEAN_AS_BYTE, inplace=True)
            return s.astype(pd.BooleanDtype(), copy=False)

        np_array = column_to_numpy_array(col_def, data_col.getDirect())
        if conv_null and (null_pair := _DTYPE_NULL_MAPPING.get(col_def.data_type)) is not None:
            nv = null_pair[0]
            pd_ex_dtype = null_pair[1]
            s = pd.Series(data=np_array, dtype=pd_ex_dtype(), copy=False)
            s.mask(s == nv, inplace=True)
        else:
            s = pd.Series(data=np_array, copy=False)
        return s
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
    pa.uint16(): pd.UInt16Dtype(),
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


def to_pandas(table: Table, cols: List[str] = None, dtype_backend: str = None, conv_null: bool = True) -> \
        pd.DataFrame:
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
        conv_null (bool): When dtype_backend is not set, whether to check for Deephaven nulls in the data and
            automatically replace them with pd.NA. default is True.

    Returns:
        a pandas DataFrame

    Raise:
        DHError
    """
    try:
        if dtype_backend is not None and not _is_dtype_backend_supported:
            raise DHError(message=f"the dtype_backend ({dtype_backend}) option is only available for pandas 2.0.0 and "
                                  f"above. {pd.__version__} is being used.")

        if dtype_backend is not None and not conv_null:
            raise DHError(message="conv_null can't be turned off when dtype_backend is either numpy_nullable or "
                                  "pyarrow")

        # if nullable dtypes (pandas or pyarrow) is requested
        if type_mapper := _PYARROW_TO_PANDAS_TYPE_MAPPERS.get(dtype_backend):
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
            series = _column_to_series(table, col_def_dict[col], conv_null)
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
    pd.BooleanDtype: _NULL_BOOLEAN_AS_BYTE,
    pd.Int8Dtype: NULL_BYTE,
    pd.Int16Dtype: NULL_SHORT,
    pd.UInt16Dtype: NULL_CHAR,
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


def _map_na(array: [np.ndarray, pd.api.extensions.ExtensionArray]):
    """Replaces the pd.NA values in the array if it is of pandas ExtensionDtype(nullable)."""
    pd_dtype = array.dtype
    if not isinstance(pd_dtype, pd.api.extensions.ExtensionDtype):
        return array

    dh_null = _EX_DTYPE_NULL_MAP.get(type(pd_dtype)) or _EX_DTYPE_NULL_MAP.get(pd_dtype)
    # To preserve NaNs in floating point arrays, Pandas doesn't distinguish NaN/Null as far as NA testing is
    # concerned, thus its fillna() method will replace both NaN/Null in the data.
    if isinstance(pd_dtype, (pd.Float32Dtype, pd.Float64Dtype)) and isinstance(getattr(array, "_data"), np.ndarray):
        np_array = array._data
        null_mask = np.logical_and(array._mask, np.logical_not(np.isnan(np_array)))
        if any(null_mask):
            np_array = np.copy(np_array)
            np_array[null_mask] = dh_null
        return np_array

    if isinstance(pd_dtype, (pd.StringDtype, pd.BooleanDtype)) or pd_dtype == pd.ArrowDtype(pa.bool_()):
        array = np.array(list(map(lambda v: dh_null if v is pd.NA else v, array)))
    elif dh_null is not None:
        array = array.fillna(dh_null)

    return array


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
            if isinstance(df.dtypes[col], pd.CategoricalDtype):
                dtype = df.dtypes[col].categories.dtype
            else:
                dtype = np_array.dtype
            dh_dtype = dtypes.from_np_dtype(dtype)
            np_array = _map_na(np_array)
            input_cols.append(_make_input_column(col, np_array, dh_dtype))

        return new_table(cols=input_cols)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, "failed to create a Deephaven Table from a pandas DataFrame.") from e

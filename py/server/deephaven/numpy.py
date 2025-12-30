#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

"""This module supports the conversion between Deephaven tables and numpy arrays."""

import re
from collections.abc import Sequence
from typing import Optional

import jpy
import numpy as np

from deephaven import DHError, dtypes, new_table
from deephaven.column import ColumnDefinition, InputColumn
from deephaven.dtypes import BusinessCalendar, DType
from deephaven.jcompat import _j_array_to_numpy_array, j_list_to_list
from deephaven.table import Table

_JColumnVectors = jpy.get_type("io.deephaven.engine.table.vectors.ColumnVectors")
_JDayOfWeek = jpy.get_type("java.time.DayOfWeek")
_JArrayList = jpy.get_type("java.util.ArrayList")


def _to_column_name(name: str) -> str:
    """Transforms the given name string into a valid table column name."""
    tmp_name = re.sub(r"\W+", " ", str(name)).strip()
    return re.sub(r"\s+", "_", tmp_name)


def _column_to_numpy_array(col_def: ColumnDefinition, j_array: jpy.JType) -> np.ndarray:
    """Produces a numpy array from the given Java array and the Table column definition.

    Args:
        col_def (ColumnDefinition): the column definition
        j_array (jpy.JType): the Java array

    Returns:
        np.ndarray

    Raises:
        DHError
    """
    try:
        return _j_array_to_numpy_array(
            col_def.data_type, j_array, conv_null=False, type_promotion=False
        )
    except DHError:
        raise
    except Exception as e:
        raise DHError(
            e, f"failed to create a numpy array for the column {col_def.name}"
        ) from e


def _columns_to_2d_numpy_array(
    col_def: ColumnDefinition, j_arrays: list[jpy.JType]
) -> np.ndarray:
    """Produces a 2d numpy array from the given Java arrays of the same component type and the Table column
    definition"""
    try:
        if col_def.data_type.is_primitive:
            np_array = np.empty(
                shape=(len(j_arrays[0]), len(j_arrays)), dtype=col_def.data_type.np_type
            )
            for i, j_array in enumerate(j_arrays):
                np_array[:, i] = np.frombuffer(j_array, col_def.data_type.np_type)
            return np_array
        else:
            np_arrays = []
            for j_array in j_arrays:
                np_arrays.append(
                    _column_to_numpy_array(col_def=col_def, j_array=j_array)
                )
            return np.stack(np_arrays, axis=1)
    except DHError:
        raise
    except Exception as e:
        raise DHError(
            e, f"failed to create a numpy array for the column {col_def.name}"
        ) from e


def _make_input_column(col: str, np_array: np.ndarray, dtype: DType) -> InputColumn:
    """Creates a InputColumn with the given column name and the numpy array."""
    return InputColumn(name=_to_column_name(col), data_type=dtype, input_data=np_array)


def to_numpy(table: Table, cols: Optional[Sequence[str]] = None) -> np.ndarray:
    """Produces a numpy array from a table.

    Note that the **entire table** is going to be cloned into memory, so the total number of entries in the table
    should be considered before blindly doing this. For large tables, consider using the Deephaven query language to
    select a subset of the table **before** using this method.

    Args:
        table (Table): the source table
        cols (Optional[Sequence[str]]): the names of the columns; if None, all columns are included, default is
            None
    Returns:
        a numpy ndarray

    Raise:
        DHError
    """

    try:
        if table.is_refreshing:
            table = table.snapshot()

        table_def = table.definition
        if not cols:
            cols = list(table_def.keys())
        else:
            diff_set = set(cols) - set(table_def.keys())
            if diff_set:
                raise DHError(message=f"columns - {list(diff_set)} not found")

        col_defs = [table_def[col] for col in cols]
        if len({col_def.data_type for col_def in col_defs}) != 1:
            raise DHError(message="columns must be of the same data type.")

        j_arrays = []
        for col_def in col_defs:
            j_arrays.append(
                _JColumnVectors.of(table.j_table, col_def.name).copyToArray()
            )
        return _columns_to_2d_numpy_array(col_defs[0], j_arrays)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, "failed to create a numpy array from the table column.") from e


def to_table(np_array: np.ndarray, cols: Sequence[str]) -> Table:
    """Produces a Deephaven table from a numpy array.

    Args:
        np_array (np.ndarray): a numpy array
        cols (Sequence[str]): a sequence of column names

    Returns:
        a Deephaven table

    Raise:
        DHError
    """

    try:
        _, *dims = np_array.shape
        if dims:
            if not cols or not dims or len(cols) != dims[0]:
                raise DHError(
                    message=f"the number of array columns {dims[0]} doesn't match "
                    f"the number of column names {len(cols)}"
                )

        input_cols = []
        dtype = dtypes.from_np_dtype(np_array.dtype)

        if len(cols) == 1:
            input_cols.append(_make_input_column(cols[0], np_array.T[0], dtype))
        else:
            for i, col in enumerate(cols):
                input_cols.append(_make_input_column(col, np_array[:, [i]].T[0], dtype))

        return new_table(cols=input_cols)
    except DHError:
        raise
    except Exception as e:
        raise DHError(
            e, "failed to create a Deephaven Table from a Pandas DataFrame."
        ) from e


def to_np_busdaycalendar(
    cal: BusinessCalendar, include_partial: bool = True
) -> np.busdaycalendar:
    """Creates a numpy business day calendar from a Java BusinessCalendar.

    Partial holidays in the business calendar are interepreted as full holidays in the numpy business day calendar.

    Args:
        cal (BusinessCalendar): the Java BusinessCalendar
        include_partial (bool): whether to include partial holidays in the numpy business day calendar, default is True

    Returns:
        a numpy busdaycalendar

    Raise:
        DHError
    """

    if not cal:
        raise DHError(message="cal must not be None")
    elif not isinstance(cal, jpy.JType) or cal.jclass != BusinessCalendar.j_type:
        raise DHError(message="cal must be a Java BusinessCalendar")

    try:
        weekend = cal.weekendDays()
        weekmask = ""
        weekmask += "0" if weekend.contains(_JDayOfWeek.MONDAY) else "1"
        weekmask += "0" if weekend.contains(_JDayOfWeek.TUESDAY) else "1"
        weekmask += "0" if weekend.contains(_JDayOfWeek.WEDNESDAY) else "1"
        weekmask += "0" if weekend.contains(_JDayOfWeek.THURSDAY) else "1"
        weekmask += "0" if weekend.contains(_JDayOfWeek.FRIDAY) else "1"
        weekmask += "0" if weekend.contains(_JDayOfWeek.SATURDAY) else "1"
        weekmask += "0" if weekend.contains(_JDayOfWeek.SUNDAY) else "1"

        # Working around jpy not supporting iteration on Sets or ArrayLists
        holiday_list = j_list_to_list(_JArrayList(cal.holidays().entrySet()))
        holidays = [
            np.datetime64(e.getKey().toString(), "D")
            for e in holiday_list
            if e.getValue().businessNanos() == 0 or include_partial
        ]

        return np.busdaycalendar(weekmask=weekmask, holidays=holidays)
    except Exception as e:
        raise DHError(
            e, "failed to create a numpy busdaycalendar from a Java BusinessCalendar."
        ) from e

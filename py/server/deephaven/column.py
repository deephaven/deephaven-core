#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module implements the Column class and functions that work with Columns. """

from dataclasses import dataclass, field
from enum import Enum
from typing import Sequence, Any

import jpy

import deephaven.dtypes as dtypes
from deephaven import DHError
from deephaven.dtypes import DType
from deephaven.dtypes import _instant_array

_JColumnHeader = jpy.get_type("io.deephaven.qst.column.header.ColumnHeader")
_JColumn = jpy.get_type("io.deephaven.qst.column.Column")
_JColumnDefinition = jpy.get_type("io.deephaven.engine.table.ColumnDefinition")
_JColumnDefinitionType = jpy.get_type("io.deephaven.engine.table.ColumnDefinition$ColumnType")
_JPrimitiveArrayConversionUtility = jpy.get_type("io.deephaven.integrations.common.PrimitiveArrayConversionUtility")


class ColumnType(Enum):
    NORMAL = _JColumnDefinitionType.Normal
    """ A regular column. """
    GROUPING = _JColumnDefinitionType.Grouping
    """ A grouping column. """
    PARTITIONING = _JColumnDefinitionType.Partitioning
    """ A partitioning column. """

    def __repr__(self):
        return self.name


@dataclass
class Column:
    """ A Column object represents a column definition in a Deephaven Table. """
    name: str
    data_type: DType
    component_type: DType = None
    column_type: ColumnType = ColumnType.NORMAL

    @property
    def j_column_header(self):
        return _JColumnHeader.of(self.name, self.data_type.qst_type)

    @property
    def j_column_definition(self):
        if hasattr(self.data_type.j_type, 'jclass'):
            j_data_type = self.data_type.j_type.jclass
        else:
            j_data_type = self.data_type.qst_type.clazz()
        j_component_type = self.component_type.qst_type.clazz() if self.component_type else None
        j_column_type = self.column_type.value
        return _JColumnDefinition.fromGenericType(self.name, j_data_type, j_component_type, j_column_type)


@dataclass
class InputColumn(Column):
    """ An InputColumn represents a user defined column with some input data. """
    input_data: Any = field(default=None)

    def __post_init__(self):
        try:
            if self.input_data is None:
                self.j_column = _JColumn.empty(self.j_column_header)
            else:
                if self.data_type.is_primitive:
                    self.j_column = _JColumn.ofUnsafe(self.name, dtypes.array(self.data_type, self.input_data,
                                                                              remap=dtypes.null_remap(self.data_type)))
                else:
                    self.j_column = _JColumn.of(self.j_column_header, dtypes.array(self.data_type, self.input_data))
        except Exception as e:
            raise DHError(e, f"failed to create an InputColumn ({self.name}).") from e


def bool_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing Boolean data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.bool_, input_data=data)


def byte_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive byte data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.byte, input_data=data)


def char_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive char data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.char, input_data=data)


def short_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive short data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.short, input_data=data)


def int_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive int data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.int32, input_data=data)


def long_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive long data.

    Args:
        name (str): the column name
        data (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.long, input_data=data)


def float_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive float data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.float32, input_data=data)


def double_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing primitive double data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.double, input_data=data)


def string_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing string data.

    Args:
        name (str): the column name
        data (Any): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.string, input_data=data)


def datetime_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing Deephaven Datetime instances.

    Args:
        name (str): the column name
        data (Any): a sequence of Datetime instances or values that can be converted to Datetime instances
            (e.g. Instant, int nanoseconds since the Epoch, str, datetime.datetime, numpy.datetime64, pandas.Timestamp).

    Returns:
        a new input column
    """
    data = _instant_array(data)
    return InputColumn(name=name, data_type=dtypes.Instant, input_data=data)


def pyobj_col(name: str, data: Sequence) -> InputColumn:
    """Creates an input column containing complex, non-primitive-like Python objects.

    Args:
        name (str): the column name
        data (Any): a sequence of Python objects

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.PyObject, input_data=data)


def jobj_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing Java objects.

    Args:
        name (str): the column name
        data (Any): a sequence of Java objects

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.JObject, input_data=data)

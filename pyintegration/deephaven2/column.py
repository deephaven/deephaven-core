#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module implements the Column class and functions that work with Columns. """

from dataclasses import dataclass, field
from enum import Enum
from typing import Sequence

import jpy

import deephaven2.dtypes as dtypes
from deephaven2 import DHError
from deephaven2.dtypes import DType

_JColumnHeader = jpy.get_type("io.deephaven.qst.column.header.ColumnHeader")
_JColumn = jpy.get_type("io.deephaven.qst.column.Column")
_JColumnDefinition = jpy.get_type("io.deephaven.engine.table.ColumnDefinition")


class ColumnType(Enum):
    NORMAL = 1
    """ A regular column. """
    GROUPING = 2
    """ A grouping column. """
    PARTITIONING = 4
    """ A partitioning column. """
    VIRTUAL = 8
    """ A virtual column. """

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
        return _JColumnDefinition.fromGenericType(self.name, self.data_type.qst_type.clazz(), self.component_type)


@dataclass
class InputColumn(Column):
    """ An InputColumn represents a user defined column with some input data. """
    input_data: any = field(default=None)

    def __post_init__(self):
        try:
            if self.input_data is None:
                self.j_column = _JColumn.empty(self.j_column_header)
            else:
                if self.data_type.is_primitive:
                    self.j_column = _JColumn.ofUnsafe(self.name, dtypes.array(self.data_type, self.input_data))
                else:
                    self.j_column = _JColumn.of(self.j_column_header, dtypes.array(self.data_type, self.input_data))
        except Exception as e:
            raise DHError(e, "failed to create an InputColumn.") from e


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
    return InputColumn(name=name, data_type=dtypes.float_, input_data=data)


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
        data (Any): a sequence of Datetime instances

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.DateTime, input_data=data)


def pyobj_col(name: str, data: Sequence) -> InputColumn:
    """ Creates an input column containing complex, non-primitive-like Python objects.

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

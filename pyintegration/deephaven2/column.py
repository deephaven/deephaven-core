#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Sequence

import jpy

import deephaven2.dtypes as dtypes
from deephaven2.dtypes import DType

_JColumnHeader = jpy.get_type("io.deephaven.qst.column.header.ColumnHeader")
_JColumn = jpy.get_type("io.deephaven.qst.column.Column")


class ColumnType(Enum):
    NORMAL = 1
    GROUPING = 2
    PARTITIONING = 4
    VIRTUAL = 8

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

@dataclass
class InputColumn(Column):
    input_data: any = field(default=None)

    @property
    def j_column(self):
        if self.input_data is None:
            return _JColumn.empty(self.j_column_header)
        else:
            if self.data_type.is_primitive:
                return _JColumn.ofUnsafe(self.name, self.data_type.array_of(self.input_data))
            else:
                return _JColumn.of(self.j_column_header, self.data_type.array_of(self.input_data))


def bool_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Java Boolean values.

    Args:
        name (str): the column name
        values (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.bool_, input_data=values)


def byte_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Java primitive byte values.

    Args:
        name (str): the column name
        values (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.byte, input_data=values)


def char_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Java primitive char values.

    Args:
        name (str): the column name
        values (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.char, input_data=values)


def short_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Java primitive short values.

    Args:
        name (str): the column name
        values (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.short, input_data=values)


def int_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Java primitive int values.

    Args:
        name (str): the column name
        values (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.int32, input_data=values)


def long_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Java primitive long values.

    Args:
        name (str): the column name
        values (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.long, input_data=values)


def float_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Java primitive float values.

    Args:
        name (str): the column name
        values (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.float_, input_data=values)


def double_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Java primitive double values.

    Args:
        name (str): the column name
        values (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.double, input_data=values)


def string_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Java String values.

    Args:
        name (str): the column name
        values (Any): a python sequence of compatible data, could be numpy array or Pandas series

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.string, input_data=values)


def datetime_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Deephaven Datetime instances.

    Args:
        name (str): the column name
        values (Any): a python sequence of Datetime instances

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.DateTime, input_data=values)


def pyobj_col(name: str, values: Sequence) -> Column:
    """ Creates an input column containing Java Pyobject instances.

    Args:
        name (str): the column name
        values (Any): a python sequence of PyObject instances

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=dtypes.PyObject, input_data=values)


def jobj_col(name: str, class_name: str, values: Sequence) -> Column:
    """ Creates an input column containing instances of the specified Java class.

    Args:
        name (str): the column name
        values (Any): a python sequence of the Java instances

    Returns:
        a new input column
    """
    return InputColumn(name=name, data_type=DType(j_name=class_name), input_data=values)

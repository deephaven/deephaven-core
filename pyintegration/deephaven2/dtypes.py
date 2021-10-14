#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
from enum import Enum
from typing import Iterable

import jpy

_qst_type = jpy.get_type("io.deephaven.qst.type.Type")
_table_tools = jpy.get_type("io.deephaven.db.tables.utils.TableTools")


def _qst_custom_type(cls_name: str):
    return _qst_type.find(_table_tools.typeFromName(cls_name))


class DType(Enum):
    """ An Enum for supported data types in Deephaven with type aliases to mirror the same ones in numpy or pyarrow.

    The complex types such as BigDecimal, DBPeriod can be called to create Java objects of the same types, e.g.
        j_big_decimal = BigDecimal(12.88)

    """
    bool_ = _qst_type.booleanType(), "java.lang.Boolean"
    byte = _qst_type.byteType(), "byte"
    short = _qst_type.shortType(), "short"
    int16 = short
    char = _qst_type.charType(), "char"
    int_ = _qst_type.intType(), "int"
    int32 = int_
    long = _qst_type.longType(), "long"
    int64 = long
    float_ = _qst_type.floatType(), "float"
    single = float_
    float32 = float_
    double = _qst_type.doubleType(), "double"
    float64 = double
    string = _qst_type.stringType(), "java.lang.String"
    BigDecimal = _qst_custom_type("java.math.BigDecimal"), "java.math.BigDecimal"
    StringSet = _qst_custom_type("io.deephaven.db.tables.libs.StringSet"), "io.deephaven.db.tables.libs.StringSet"
    DBDateTime = _qst_custom_type("io.deephaven.db.tables.utils.DBDateTime"), "io.deephaven.db.tables.utils.DBDateTime"
    DBPeriod = _qst_custom_type("io.deephaven.db.tables.utils.DBPeriod"), "io.deephaven.db.tables.utils.DBPeriod"

    def __new__(cls, *args, **kwargs):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, qst_type, j_name):
        self._qst_type = qst_type
        self._j_name = j_name
        self._j_type = jpy.get_type(j_name)

    def __call__(self, *args, **kwargs):
        return self._j_type(*args, **kwargs)

    @property
    def qst_type(self):
        """

        Returns: the QST type

        """
        return self._qst_type

    @property
    def j_type(self):
        """

        Returns: the mapped Java type

        """
        return self._j_type

    def array(self, size: int):
        """

        Args:
            size:

        Returns:

        """
        return jpy.array(self._j_name, size)

    def array_from(self, values: Iterable):
        """

        Args:
            values: a Python iterable

        Returns: a Java array of the same data type populated with values from the Python iterable

        """
        j_array = jpy.array(self._j_name, len(values))
        for i, v in enumerate(values):
            j_array[i] = v
        return j_array


bool_ = DType.bool_
byte = DType.byte
short = DType.short
int16 = short
char = DType.char
int_ = DType.int_
int32 = DType.int32
long = DType.long
int64 = long
float_ = DType.float_
single = float_
float32 = float_
double = DType.double
float64 = double
string = DType.string
BigDecimal = DType.BigDecimal
StringSet = DType.StringSet
DBDateTime = DType.DBDateTime
DBPeriod = DType.DBPeriod

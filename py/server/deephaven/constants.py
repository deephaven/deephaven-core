#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" The module defines the global constants including Deephaven's special numerical values. Other constants are defined
at the individual module level because they are only locally applicable. """

import jpy

_JQueryConstants = jpy.get_type("io.deephaven.util.QueryConstants")

# Deephaven Special Null values for primitive types
NULL_BOOLEAN = _JQueryConstants.NULL_BOOLEAN
""" Null boolean value. """
NULL_CHAR = _JQueryConstants.NULL_CHAR
""" Null char value. """
MIN_CHAR = _JQueryConstants.MIN_CHAR
""" Minimum value of type char. """
MAX_CHAR = _JQueryConstants.MAX_CHAR
""" Maximum value of type char. """

NULL_BYTE = _JQueryConstants.NULL_BYTE
""" Null byte value. """
MIN_BYTE = _JQueryConstants.MIN_BYTE
""" Minimum value of type byte. """
MAX_BYTE = _JQueryConstants.MAX_BYTE
""" Maximum value of type byte. """

NULL_SHORT = _JQueryConstants.NULL_SHORT
""" Null short value. """
MIN_SHORT = _JQueryConstants.MIN_SHORT
""" Minimum value of type short. """
MAX_SHORT = _JQueryConstants.MAX_SHORT
""" Maximum value of type short. """

NULL_INT = _JQueryConstants.NULL_INT
""" Null int value. """
MIN_INT = _JQueryConstants.MIN_INT
""" Minimum value of type int. """
MAX_INT = _JQueryConstants.MAX_INT
""" Maximum value of type int. """

NULL_LONG = _JQueryConstants.NULL_LONG
""" Null long value. """
MIN_LONG = _JQueryConstants.MIN_LONG
""" Minimum value of type long. """
MAX_LONG = _JQueryConstants.MAX_LONG
""" Maximum value of type long. """

NULL_FLOAT = _JQueryConstants.NULL_FLOAT
""" Null float value. """
NAN_FLOAT = _JQueryConstants.NAN_FLOAT
""" Not-a-number (Nan) of type float. """
NEG_INFINITY_FLOAT = _JQueryConstants.NEG_INFINITY_FLOAT
""" Negative infinity of type float. """
POS_INFINITY_FLOAT = _JQueryConstants.POS_INFINITY_FLOAT
""" Positive infinity of type float. """
MIN_FLOAT = _JQueryConstants.MIN_FLOAT
""" Minimum value of type float. """
MAX_FLOAT = _JQueryConstants.MAX_FLOAT
""" Maximum value of type float. """
MIN_FINITE_FLOAT = _JQueryConstants.MIN_FINITE_FLOAT
""" Minimum finite value of type float. """
MAX_FINITE_FLOAT = _JQueryConstants.MAX_FINITE_FLOAT
""" Maximum finite value of type float. """
MIN_POS_FLOAT = _JQueryConstants.MIN_POS_FLOAT
""" Minimum positive value of type float. """

NULL_DOUBLE = _JQueryConstants.NULL_DOUBLE
""" Null double value. """
NAN_DOUBLE = _JQueryConstants.NAN_DOUBLE
""" Not-a-number (Nan) of type double. """
NEG_INFINITY_DOUBLE = _JQueryConstants.NEG_INFINITY_DOUBLE
""" Negative infinity of type double. """
POS_INFINITY_DOUBLE = _JQueryConstants.POS_INFINITY_DOUBLE
""" Positive infinity of type double. """
MIN_DOUBLE = _JQueryConstants.MIN_DOUBLE
""" Minimum value of type double. """
MAX_DOUBLE = _JQueryConstants.MAX_DOUBLE
""" Maximum value of type double. """
MIN_FINITE_DOUBLE = _JQueryConstants.MIN_FINITE_DOUBLE
""" Minimum finite value of type double. """
MAX_FINITE_DOUBLE = _JQueryConstants.MAX_FINITE_DOUBLE
""" Maximum finite value of type double. """
MIN_POS_DOUBLE = _JQueryConstants.MIN_POS_DOUBLE
""" Minimum positive value of type double. """

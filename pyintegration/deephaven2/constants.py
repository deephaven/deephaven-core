#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from enum import Enum, auto
import jpy

_JQueryConstants = jpy.get_type("io.deephaven.util.QueryConstants")
_JTimeZone = jpy.get_type("io.deephaven.time.TimeZone")


class SortDirection(Enum):
    """An enum defining the sorting orders."""
    DESCENDING = auto()
    """"""
    ASCENDING = auto()
    """"""


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


class TimeZone(Enum):
    """ A Enum for known time zones. """
    NY = _JTimeZone.TZ_NY
    """ America/New_York """
    ET = _JTimeZone.TZ_ET
    """ America/New_York """
    MN = _JTimeZone.TZ_MN
    """ America/Chicago """
    CT = _JTimeZone.TZ_CT
    """ America/Chicago """
    MT = _JTimeZone.TZ_MT
    """ America/Denver """
    PT = _JTimeZone.TZ_PT
    """ America/Los_Angeles """
    HI = _JTimeZone.TZ_HI
    """ Pacific/Honolulu """
    BT = _JTimeZone.TZ_BT
    """ America/Sao_Paulo """
    KR = _JTimeZone.TZ_KR
    """ Asia/Seoul """
    HK = _JTimeZone.TZ_HK
    """ Asia/Hong_Kong """
    JP = _JTimeZone.TZ_JP
    """ Asia/Tokyo """
    AT = _JTimeZone.TZ_AT
    """ Canada/Atlantic """
    NF = _JTimeZone.TZ_NF
    """ Canada/Newfoundland """
    AL = _JTimeZone.TZ_AL
    """ America/Anchorage """
    IN = _JTimeZone.TZ_IN
    """ Asia/Kolkata """
    CE = _JTimeZone.TZ_CE
    """ Europe/Berlin """
    SG = _JTimeZone.TZ_SG
    """ Asia/Singapore """
    LON = _JTimeZone.TZ_LON
    """ Europe/London """
    MOS = _JTimeZone.TZ_MOS
    """ Europe/Moscow """
    SHG = _JTimeZone.TZ_SHG
    """ Asia/Shanghai """
    CH = _JTimeZone.TZ_CH
    """ Europe/Zurich """
    NL = _JTimeZone.TZ_NL
    """ Europe/Amsterdam """
    TW = _JTimeZone.TZ_TW
    """ Asia/Taipei """
    SYD = _JTimeZone.TZ_SYD
    """ Australia/Sydney """
    UTC = _JTimeZone.TZ_UTC
    """ UTC """

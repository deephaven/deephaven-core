#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module defines the classes for Deephaven DateTime and Period. """

from typing import Any, Sequence, Callable

from deephaven2 import DHError
from deephaven2.dtypes import DType

_DHDateTime = DType(j_name="io.deephaven.time.DateTime")
_DHPeriod = DType(j_name="io.deephaven.time.Period")


class DateTime:
    """ This class defines a Deephaven DateTime object. """
    is_primitive = False
    j_type = _DHDateTime.j_type
    qst_type = _DHDateTime.qst_type

    def __init__(self, v: Any):
        if isinstance(v, _DHDateTime.j_type):
            self.j_datetime = v
        else:
            # TODO conversion support from Python built-in datetime, string datetime representation etc.
            self.j_datetime = _DHDateTime(v)

    def __eq__(self, other):
        if isinstance(other, DateTime):
            return self.j_datetime.equals(other.j_datetime)

        return False

    def __repr__(self):
        return str(self.j_datetime)

    @classmethod
    def now(cls):
        """ Returns a DataTime object initialized to the current time. """
        return DateTime(_DHDateTime.j_type.now())

    @classmethod
    def array(cls, size: int):
        """ Creates a Java array of the Deephaven DateTime data type of the specified size.

        Args:
            size (int): the size of the array

        Returns:
            a Java array

        Raises:
            DHError
        """
        return _DHDateTime.array(size)

    @classmethod
    def array_from(cls, seq: Sequence, remap: Callable[[Any], Any] = None):
        """ Creates a Java array of Deephaven DateTime instances from a sequence.

        Args:
            seq: a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.
            remap (optional): a callable that takes one value and maps it to another, for handling the translation of
                special DH values such as NULL_INT, NAN_INT between Python and the DH engine

        Returns:
            a Java array

        Raises:
            DHError
        """

        new_seq = []
        for v in seq:
            if v is None:
                new_seq.append(None)
            elif isinstance(v, DateTime):
                new_seq.append(v.j_datetime)
            elif isinstance(v, _DHDateTime.j_type):
                new_seq.append(v)
            else:
                raise DHError(message="Not a valid datetime")

        return _DHDateTime.array_from(seq=new_seq, remap=remap)


class Period:
    """ This class defines a Deephaven Period object. """
    is_primitive = False
    j_type = _DHPeriod.j_type
    qst_type = _DHPeriod.qst_type

    def __init__(self, v: Any):
        if v is None:
            self.j_period = None
        else:
            if isinstance(v, _DHPeriod.j_type):
                self.j_period = v
            else:
                # TODO? conversion support from Python timedelta, etc.
                self.j_period = _DHPeriod(v)

    def __repr__(self):
        return str(self.j_period)
# distutils: language = c++

from libc.stdint cimport int64_t

# TODO: provide an include path to avoid hardcoding all over the place the common prefix.
cdef extern from "../../../../cpp-client/deephaven/client/src/types.cc":
    pass

cdef extern from "../../../../cpp-client/deephaven/client/include/public/deephaven/client/types.h" namespace "deephaven::client":
    cdef cppclass CDateTime "deephaven::client::DateTime" nogil:
        CDateTime() except +
        CDateTime(int64_t) except +
        CDateTime(int, int, int) except +
        CDateTime(int, int, int, int, int, int) except +
        CDateTime(int, int, int, int, int, int, long) except +
        int64_t nanos()
        @staticmethod
        CDateTime fromNanos(long)

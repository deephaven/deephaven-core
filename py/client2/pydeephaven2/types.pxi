# cython: profile=False
# distutils: language = c++
# cython: language_level = 3

from libc.stdint cimport int64_t

from pydeephaven2.includes.libdeephaven cimport CDateTime

cdef class DateTime:
    cdef CDateTime* c_datetime

    def __cinit__(self, int year=-1, int month=-1, int day=-1, int hour=-1, int minute=-1, int second=-1, long nanos=-1):
        if year == -1 and month == -1 and day == -1 and hour == -1 and minute == -1 and second == -1:
            if nanos == -1:
               raise ValueError('nanos must be >= 0.')
            self.c_datetime = new CDateTime(nanos)
            return
        if year != -1 or month != -1 or day != -1 or hour != -1 or minute != -1 or second != -1:
            raise ValueError('Either all of (year,month,day,hour,minute,second) are provided or none of them are.')
        if nanos == -1:
            nanos = 0
        self.c_datetime = new CDateTime(year, month, day, hour, minute, second, nanos)

    def nanos(self):
        return self.c_datetime.nanos()

    def __dealloc__(self):
        del self.c_datetime

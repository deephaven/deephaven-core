#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides access to the Deephaven server configuration. """
import jpy

from deephaven.dtypes import TimeZone

_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")


def get_server_timezone() -> TimeZone:
    """ Returns the server's time zone. """
    return _JDateTimeUtils.timeZone()

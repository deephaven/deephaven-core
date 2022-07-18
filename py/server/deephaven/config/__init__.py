#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides access to the Deephaven server configuration. """
import jpy

from deephaven import DHError
from deephaven.time import TimeZone

_JDHConfig = jpy.get_type("io.deephaven.configuration.Configuration")
_JDateTimeZone = jpy.get_type("org.joda.time.DateTimeZone")


def get_log_dir() -> str:
    """ Returns the server's log directory. """
    try:
        return _JDHConfig.getInstance().getLogDir()
    except Exception as e:
        raise DHError(e, "failed to get the server's log directory.") from e


def get_server_timezone() -> TimeZone:
    """ Returns the server's time zone. """
    try:
        j_timezone = _JDateTimeZone.forTimeZone(_JDHConfig.getInstance().getServerTimezone())
        for tz in TimeZone:
            if j_timezone == tz.value.getTimeZone():
                return tz
        raise NotImplementedError("can't find the time zone in the TImeZone Enum.")
    except Exception as e:
        raise DHError(e, message=f"failed to find a recognized time zone") from e

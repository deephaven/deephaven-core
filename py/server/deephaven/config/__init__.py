#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides access to the Deephaven server configuration. """
import jpy

from deephaven import DHError
from deephaven.time import TimeZone

_JDHConfig = jpy.get_type("io.deephaven.configuration.Configuration")


def get_server_timezone() -> TimeZone:
    """ Returns the server's time zone. """
    try:
        j_timezone = _JDHConfig.getInstance().getServerTimezone()
        for tz in TimeZone:
            if j_timezone.getZoneId() == tz.value.getZoneId():
                return tz
        raise NotImplementedError("can't find the time zone in the TimeZone Enum.")
    except Exception as e:
        raise DHError(e, message=f"failed to find a recognized time zone") from e

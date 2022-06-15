#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

class DHError(Exception):
    """ A custom exception class used by pydeephaven.
    """
    def __init__(self, message=""):
        super().__init__(message)
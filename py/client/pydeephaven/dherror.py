#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""This module defines a custom exception class for the package."""


class DHError(Exception):
    """A custom exception class used by pydeephaven."""

    def __init__(self, message=""):
        super().__init__(message)

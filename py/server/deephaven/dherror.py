#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module defines a custom exception for the Deephaven Python Integration Package.

The custom exception is named DHError. It encapsulates exceptions thrown by the Deephaven engine and the
Python/Java integration layer and provides 3 convenient properties: root_cause, compact_traceback, and
traceback for easy debugging.

"""
import re
import traceback


class DHError(Exception):
    """ The custom exception class for the Deephaven Python package.

    This exception can be raised due to user errors or system errors when Deephaven resources and functions
    are accessed, for example, during reading a CSV/Parquet file into a Deephaven table or performing an
    aggregation or join operation on Deephaven tables. It is a good practice for Python code to catch this
    exception and handle it appropriately.
    """

    def __init__(self, cause=None, message=""):
        if isinstance(cause, str) and message == "":
            # Saves a lot of debugging headache when library code incorrectly creates something like:
            # raise DHError("My error message here")
            message = cause
            cause = None
        self._message = message
        self._traceback = traceback.format_exc()
        self._cause = cause
        tb_lines = self._traceback.splitlines()
        self._root_cause = ""
        self._compact_tb = []
        for_compact_tb = True
        for tb_ln in tb_lines:
            if tb_ln.startswith("caused by"):
                self._root_cause = tb_ln.split("by")[1].strip()
                if tb_ln.strip().endswith(":"):
                    self._compact_tb.append(tb_ln[:-1].strip())
                else:
                    self._compact_tb.append(tb_ln)
            elif re.match("^.*Error:", tb_ln):
                self._root_cause = tb_ln
                self._compact_tb.append(tb_ln)
                for_compact_tb = False
            elif tb_ln.startswith("Exception message"):
                self._root_cause = tb_ln.split(":")[1] if ":" in tb_ln else tb_ln
                self._root_cause = self._root_cause.strip()
                self._compact_tb[-1] = self._compact_tb[-1] + f" {self._root_cause}"

            if for_compact_tb:
                self._compact_tb.append(tb_ln)

        if not self._root_cause:
            self._root_cause = self._message

    @property
    def root_cause(self):
        """ The root cause of the exception. """
        return self._root_cause

    @property
    def traceback(self):
        """ The traceback of the exception. """
        return self._traceback

    @property
    def compact_traceback(self) -> str:
        """ The compact traceback of the exception. """
        return "\n".join(self._compact_tb)

    def __str__(self):
        return f"{self._message} : {self._root_cause}\n{self._traceback}"

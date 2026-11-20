#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

"""This module defines a custom exception for the Deephaven Python Integration Package.

The custom exception is named DHError. It encapsulates exceptions thrown by the Deephaven engine and the
Python/Java integration layer and provides 3 convenient properties: root_cause, compact_traceback, and
traceback for easy debugging.

"""

import re
import traceback

# Matches a Java cause line, such as 'caused by java.lang.RuntimeException: ...'. The class name must contain a dot.
# Without that rule, a line in a Python exception message that starts with 'caused by' would count as a Java cause.
_JAVA_CAUSE_RE = re.compile(r"^caused by (?:[\w$]+\.)+[\w$]+(?::|$)")


class DHError(Exception):
    """The custom exception class for the Deephaven Python package.

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
            if _JAVA_CAUSE_RE.match(tb_ln):
                self._root_cause = tb_ln[len("caused by") :].strip()
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

        self._use_python_exception_as_root_cause(tb_lines)

        if not self._root_cause:
            self._root_cause = self._message

    def _use_python_exception_as_root_cause(self, tb_lines: list[str]) -> None:
        """Uses the Python exception line as the root cause when the deepest Java cause is a Python error.

        This happens when a Python function called from a query string raises. Without this method, the root cause
        is the 'Error in Python interpreter:' line for any exception whose name doesn't end in 'Error', such as
        Exception or KeyboardInterrupt.
        """
        # Java reports a Python error as 'caused by java.lang.RuntimeException: Error in Python interpreter:'.
        # The Python traceback comes next, then the Java stack frames, which start with a tab and 'at '.
        caused_by = [i for i, ln in enumerate(tb_lines) if _JAVA_CAUSE_RE.match(ln)]
        if not caused_by or not tb_lines[caused_by[-1]].rstrip().endswith(
            "Error in Python interpreter:"
        ):
            return

        py_tb_lines = []
        for tb_ln in tb_lines[caused_by[-1] + 1 :]:
            if tb_ln.startswith("\tat "):
                break
            py_tb_lines.append(tb_ln)

        # The exception line is the first unindented line after the last 'File' line. The last non-blank line would
        # be wrong when the exception message has more than one line.
        file_lines = [
            i for i, ln in enumerate(py_tb_lines) if ln.startswith('  File "')
        ]
        if not file_lines:
            return
        py_exc_line = next(
            (
                ln
                for ln in py_tb_lines[file_lines[-1] + 1 :]
                if ln.strip() and not ln.startswith(" ")
            ),
            "",
        )
        if not py_exc_line:
            return

        self._root_cause = py_exc_line
        # The main loop already added the line when the exception name ends in 'Error'.
        if not self._compact_tb or self._compact_tb[-1] != py_exc_line:
            self._compact_tb.append(py_exc_line)

    @property
    def root_cause(self):
        """The root cause of the exception."""
        return self._root_cause

    @property
    def traceback(self):
        """The traceback of the exception."""
        return self._traceback

    @property
    def compact_traceback(self) -> str:
        """The compact traceback of the exception."""
        return "\n".join(self._compact_tb)

    def __str__(self):
        return f"{self._message} : {self._root_cause}\n{self._traceback}"

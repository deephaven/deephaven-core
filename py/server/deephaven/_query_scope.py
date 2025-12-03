#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
"""
This is an internal package for providing a context manager that sets a thread-local addition to the script session
query scope using Python's globals() and locals().

This can be called *only* from table.py or execution_context.py.  Using this from any other file does not work!
"""

import contextlib
import inspect
from typing import TYPE_CHECKING, Any, cast

import jpy

if TYPE_CHECKING:
    from typing_extensions import TypeAlias  # novermin  # noqa

from deephaven import DHError
from deephaven._jpy import strict_cast

_JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
_JScriptSessionQueryScope = jpy.get_type(
    "io.deephaven.engine.util.AbstractScriptSession$ScriptSessionQueryScope"
)
_JPythonScriptSession = cast(
    type[Any], jpy.get_type("io.deephaven.integrations.python.PythonDeephavenSession")
)  # type: TypeAlias


def _j_py_script_session() -> _JPythonScriptSession:
    j_execution_context = _JExecutionContext.getContext()
    j_query_scope = j_execution_context.getQueryScope()
    try:
        j_script_session_query_scope = strict_cast(
            j_query_scope, _JScriptSessionQueryScope
        )
        return strict_cast(
            j_script_session_query_scope.scriptSession(), _JPythonScriptSession
        )
    except DHError:
        return None


@contextlib.contextmanager
def query_scope_ctx():
    """A context manager to set/unset query scope based on the scope of the most immediate caller code that invokes
    Table operations.

    This can be called *only* from table.py or execution_context.py.  Using this from any other file does not work!
    """

    # locate the innermost Deephaven frame (i.e. any of the table operation methods that use this context manager)
    outer_frames = inspect.getouterframes(inspect.currentframe())[1:]

    table_file = __file__.replace("_query_scope.py", "table.py")
    exec_ctx_file = __file__.replace("_query_scope.py", "execution_context.py")

    for i, (frame, filename, *_) in enumerate(outer_frames):
        if filename and (filename == table_file or filename == exec_ctx_file):
            break
    if i >= len(outer_frames):
        # we didn't match anything, just bail out without an error
        yield

    # combine the immediate caller's globals and locals into a single dict and use it as the query scope
    caller_frame = outer_frames[i + 1].frame
    function = outer_frames[i + 1].function
    j_py_script_session = _j_py_script_session()
    if j_py_script_session and (len(outer_frames) > i + 2 or function != "<module>"):
        scope_dict = caller_frame.f_globals.copy()
        scope_dict.update(caller_frame.f_locals)
        j_py_script_session.pushScope(scope_dict)
        try:
            yield
        finally:
            j_py_script_session.popScope()
    else:
        # in the __main__ module, use the default main global scope
        yield

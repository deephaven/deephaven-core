#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""
This is an internal package for providing a context manager that sets a thread-local addition to the script session
query scope using Python's globals() and locals().

This can be called *only* from table.py or execution_context.py.  Using this from any other file does not work!
"""

import contextlib
import inspect
from typing import Any, TypeAlias, cast

import jpy

from deephaven import DHError
from deephaven._jpy import strict_cast

_JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
_JScriptSessionQueryScope = jpy.get_type(
    "io.deephaven.engine.util.AbstractScriptSession$ScriptSessionQueryScope"
)
_JPythonScriptSession: TypeAlias = cast(
    type[Any], jpy.get_type("io.deephaven.integrations.python.PythonDeephavenSession")
)


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


_THIS_FILE = _j_py_script_session.__code__.co_filename
_DH_FILES = frozenset(
    _THIS_FILE.replace("_query_scope.py", name)
    for name in ("table.py", "execution_context.py")
)


@contextlib.contextmanager
def query_scope_ctx():
    """A context manager to set/unset query scope based on the scope of the most immediate caller code that invokes
    Table operations.

    This can be called *only* from table.py or execution_context.py.  Using this from any other file does not work!
    To call it from another module, add that module to _DH_FILES.
    """

    # Read f_back directly. Do not use inspect.stack() or inspect.getouterframes(). They call inspect.getmodule() on
    # each frame, and for a frame whose file maps to no module, such as console code ('<string>') or '<frozen runpy>',
    # getmodule() rescans all of sys.modules. Every table operation would then slow down as more modules load.

    # locate the innermost Deephaven frame (i.e. any of the table operation methods that use this context manager)
    frame = inspect.currentframe()
    while frame is not None and frame.f_code.co_filename not in _DH_FILES:
        frame = frame.f_back

    # walk past the rest of the Deephaven frames, so that a private helper that enters this context manager on behalf
    # of a public table operation still gets the user's scope instead of the public method's
    while frame is not None and frame.f_code.co_filename in _DH_FILES:
        frame = frame.f_back

    caller_frame = frame
    if caller_frame is None:
        # we didn't match anything, just bail out without an error
        yield
        return

    # combine the immediate caller's globals and locals into a single dict and use it as the query scope
    j_py_script_session = _j_py_script_session()
    if j_py_script_session and (
        caller_frame.f_back is not None or caller_frame.f_code.co_name != "<module>"
    ):
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

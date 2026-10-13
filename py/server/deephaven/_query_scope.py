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


# The source files of the table operations that use query_scope_ctx. A frame's code filename is compared against
# them, so take them from a code object in this package rather than from __file__, which names the .pyc file when the
# package is installed without sources.
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
    """

    # Walk f_back by hand instead of calling inspect.getouterframes(). getouterframes() builds source info for every
    # frame and calls inspect.getmodule() on each one. For a frame whose file maps to no module, such as <frozen runpy>
    # under `python -m` or pytest, getmodule() rescans all of sys.modules. That made every table operation cost time
    # that grew with len(sys.modules).

    # locate the innermost Deephaven frame (i.e. any of the table operation methods that use this context manager)
    frame = inspect.currentframe()
    while frame is not None and frame.f_code.co_filename not in _DH_FILES:
        frame = frame.f_back

    caller_frame = frame.f_back if frame is not None else None
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

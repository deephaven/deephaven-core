#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module gives users the ability to directly manage the Deephaven query execution context on threads, which is
critical for applications to correctly launch deferred query evaluations, such as table update operations in threads.
"""

from typing import Sequence, Union
from contextlib import ContextDecorator

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.jcompat import to_sequence
from deephaven.update_graph import UpdateGraph

_JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")


class ExecutionContext(JObjectWrapper, ContextDecorator):
    """An ExecutionContext represents a specific combination of query library, query compiler, and query scope under
    which a query is evaluated.

    A default, systemic ExecutionContext is created when a Deephaven script session is initialized, and it is used to
    evaluate queries submitted through this script session. However, to be able to evaluate a query in a deferred
    manner, such as in a different thread from the script session thread, an application must explicitly obtain or
    build a ExecutionContext and use it as a context manager to enclose the query in the body of the with statement.

    Note that, a ExecutionContext can be shared among many threads. The most typical use pattern would be to obtain
    the script session's systemic ExecutionContext and use it to wrap a query run in a thread created by the user.
    """
    j_object_type = _JExecutionContext

    @property
    def j_object(self) -> jpy.JType:
        return self.j_exec_ctx

    @property
    def update_graph(self) -> UpdateGraph:
        return UpdateGraph(j_update_graph=self.j_exec_ctx.getUpdateGraph())

    def __init__(self, j_exec_ctx):
        self.j_exec_ctx = j_exec_ctx

    def __enter__(self):
        self._j_safe_closable = self.j_exec_ctx.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._j_safe_closable.close()


def make_user_exec_ctx(freeze_vars: Union[str, Sequence[str]] = None) -> ExecutionContext:
    """Makes a new ExecutionContext based off the current thread's ExecutionContext. The optional parameter
    freeze_vars should be a list of names in the current query scope. If it is provided, the resulting ExecutionContext
    will include a new query scope that is made up of only these names together with their current values. Future
    changes to the values of these names will not be visible in the new ExecutionContext.

    Args:
        freeze_vars(Union[str, Sequence[str]]): the names of the vars in the current query scope to be frozen in
            the new one

    Returns:
        a ExecutionContext

    Raises:
        DHError
    """
    freeze_vars = to_sequence(freeze_vars)

    if not freeze_vars:
        return ExecutionContext(j_exec_ctx=_JExecutionContext.makeExecutionContext(False))
    else:
        try:
            j_exec_ctx = (_JExecutionContext.newBuilder()
                          .captureQueryCompiler()
                          .captureQueryLibrary()
                          .captureQueryScopeVars(*freeze_vars)
                          .captureUpdateGraph()
                          .build())
            return ExecutionContext(j_exec_ctx=j_exec_ctx)
        except Exception as e:
            raise DHError(message="failed to make a new ExecutionContext") from e


def get_exec_ctx() -> ExecutionContext:
    """Returns the current thread's ExecutionContext.

    Returns:
        a ExecutionContext
    """
    return ExecutionContext(j_exec_ctx=_JExecutionContext.getContext())

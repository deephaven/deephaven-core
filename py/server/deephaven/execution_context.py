#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module gives users the ability to directly manage the Deephaven query execution context on threads, which is
critical for applications to correctly launch deferred query evaluations, such as table update operations in threads.
"""
from __future__ import annotations

import jpy

from deephaven._wrapper import JObjectWrapper

_JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")


class ExecutionContext(JObjectWrapper):
    """An ExecutionContext represents a specific combination of query library, query compiler, and query scope under
    which a query is evaluated.

    A default, systemic ExecutionContext is created when a Deephaven script session is initialized, and it is used to
    evaluate queries submitted through this script session. However, to be able to evaluate a query in a deferred
    manner, such as in a different thread from the script session thread, an application must explicitly obtain or
    build a ExecutionContext and either (1) install it on the thread or (2) use it as a context manager to enclose
    the query in the body of the with statement.

    Note that, a ExecutionContext can be shared among many threads. The most typical use pattern would be to obtain
    the script session's systemic ExecutionContext and set it on a thread explicitly created by the user or simply
    use it to wrap a query run in that thread.
    """
    j_object_type = _JExecutionContext

    @property
    def j_object(self) -> jpy.JType:
        return self.j_exec_ctx

    def __init__(self, j_exec_ctx):
        self.j_exec_ctx = j_exec_ctx

    def __enter__(self):
        self._j_safe_closable = self.j_exec_ctx.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._j_safe_closable.close()


def make_user_exec_ctx() -> ExecutionContext:
    """Makes a new ExecutionContext based off the current thread's ExecutionContext.

    Returns:
        a ExecutionContext
    """
    return ExecutionContext(j_exec_ctx=_JExecutionContext.makeExecutionContext(False))


def get_exec_ctx() -> ExecutionContext:
    """Returns the current thread's ExecutionContext.

    Returns:
        a ExecutionContext
    """
    return ExecutionContext(j_exec_ctx=_JExecutionContext.getContext())


def set_exec_ctx(exec_ctx: ExecutionContext) -> None:
    """Sets the current thread's ExecutionContext.

    Args:
        exec_ctx (ExecutionContext): the ExecutionContext to be installed on the current thread
    """
    _JExecutionContext.setContext(exec_ctx.j_exec_ctx)

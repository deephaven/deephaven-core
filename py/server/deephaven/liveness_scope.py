#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

"""This module gives the users a finer degree of control over when to clean up unreferenced nodes in the query update
graph instead of solely relying on garbage collection."""
import contextlib

import jpy
from deephaven._wrapper import JObjectWrapper

_JLivenessScopeStack = jpy.get_type("io.deephaven.engine.liveness.LivenessScopeStack")
_JLivenessScope = jpy.get_type("io.deephaven.engine.liveness.LivenessScope")


class LivenessScope(JObjectWrapper):
    """A LivenessScope automatically manages reference counting of tables and other query resources that are
    created in it. It should not be instantiated directly but rather through the 'liveness_scope' context manager.
    """
    j_object_type = _JLivenessScope

    def __init__(self, j_scope: jpy.JType):
        self.j_scope = j_scope

    @property
    def j_object(self) -> jpy.JType:
        return self.j_scope

    def preserve(self, wrapper: JObjectWrapper) -> None:
        """Preserves a query graph node (usually a Table) to keep it live for the outer scope."""
        _JLivenessScopeStack.pop(self.j_scope)
        _JLivenessScopeStack.peek().manage(wrapper.j_object)
        _JLivenessScopeStack.push(self.j_scope)


@contextlib.contextmanager
def liveness_scope() -> LivenessScope:
    """Creates a LivenessScope for running a block of code and releases all the query graph resources upon exit. It
    can be used in a nested way.

    Returns:
        a LivenessScope
    """
    j_scope = _JLivenessScope()
    _JLivenessScopeStack.push(j_scope)
    try:
        yield LivenessScope(j_scope=j_scope)
    finally:
        _JLivenessScopeStack.pop(j_scope)
        j_scope.release()

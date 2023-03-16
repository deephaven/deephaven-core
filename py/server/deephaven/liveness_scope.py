#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

"""This module gives the users a finer degree of control over when to clean up unreferenced nodes in the query update
graph instead of solely relying on garbage collection."""
import contextlib

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper

_JLivenessScopeStack = jpy.get_type("io.deephaven.engine.liveness.LivenessScopeStack")
_JLivenessScope = jpy.get_type("io.deephaven.engine.liveness.LivenessScope")


class LivenessScope(JObjectWrapper):
    """A LivenessScope automatically manages reference counting of tables and other query resources that are
    created in it. It implements the context manager protocol and thus can be used in the with statement.

    Note, LivenessScope should not be instantiated directly but rather through the 'liveness_scope' function.
    """
    j_object_type = _JLivenessScope

    def __init__(self, j_scope: jpy.JType):
        self.j_scope = j_scope

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Closes the LivenessScope and releases all the query graph resources.

        Raises:
            DHError
        """
        if self.j_scope:
            try:
                _JLivenessScopeStack.pop(self.j_scope)
                self.j_scope.release()
                self.j_scope = None
            except Exception as e:
                raise DHError(e, message="failed to close the LivenessScope.")

    @property
    def j_object(self) -> jpy.JType:
        return self.j_scope

    def preserve(self, wrapper: JObjectWrapper) -> None:
        """Preserves a query graph node (usually a Table) to keep it live for the outer scope.

        Args:
            wrapper (JObjectWrapper): a wrapped Java object such as a Table

        Raises:
            DHError
        """
        try:
            _JLivenessScopeStack.pop(self.j_scope)
            _JLivenessScopeStack.peek().manage(wrapper.j_object)
            _JLivenessScopeStack.push(self.j_scope)
        except Exception as e:
            raise DHError(e, message="failed to preserve a wrapped object in this LivenessScope.")


def liveness_scope() -> LivenessScope:
    """Creates a LivenessScope for running a block of code.

    Returns:
        a LivenessScope
    """
    j_scope = _JLivenessScope()
    _JLivenessScopeStack.push(j_scope)
    return LivenessScope(j_scope=j_scope)

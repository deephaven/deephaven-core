#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

"""This module gives the users a finer degree of control over when to clean up unreferenced nodes in the query update
graph instead of solely relying on garbage collection."""
import contextlib

import jpy

from typing import Union
from warnings import warn

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper

_JLivenessScopeStack = jpy.get_type("io.deephaven.engine.liveness.LivenessScopeStack")
_JLivenessScope = jpy.get_type("io.deephaven.engine.liveness.LivenessScope")
_JLivenessReferent = jpy.get_type("io.deephaven.engine.liveness.LivenessReferent")


def _push(scope: _JLivenessScope):
    _JLivenessScopeStack.push(scope)


def _pop(scope: _JLivenessScope):
    try:
        _JLivenessScopeStack.pop(scope)
    except Exception as e:
        raise DHError(e, message="failed to pop the LivenessScope from the stack.")


def _unwrap_to_liveness_referent(referent: Union[JObjectWrapper, jpy.JType]) -> jpy.JType:
    if isinstance(referent, jpy.JType) and _JLivenessReferent.jclass.isInstance(referent):
        return referent
    if isinstance(referent, JObjectWrapper):
        _unwrap_to_liveness_referent(referent.j_object)
    raise DHError("Provided referent isn't a LivenessReferent or a JObjectWrapper around one")

class LivenessScopeFrame:
    """Helper class to support pushing a liveness scope without forcing it to be closed when finished."""
    def __init__(self, scope: "deephaven.liveness_scope.LivenessScope", close_after_block: bool):
        self.close_after_pop = close_after_block
        self.scope = scope

    def __enter__(self):
        _push(self.scope.j_scope)
        return self.scope

    def __exit__(self, exc_type, exc_val, exc_tb):
        _pop(self.scope.j_scope)
        if self.close_after_pop:
            self.scope.close()


class LivenessScope(JObjectWrapper):
    """A LivenessScope automatically manages reference counting of tables and other query resources that are
    created in it. It implements the context manager protocol and thus can be used in the with statement.

    Note, LivenessScope should not be instantiated directly but rather through the 'liveness_scope' function.
    """
    j_object_type = _JLivenessScope

    def __init__(self, j_scope: jpy.JType):
        super().__init__(j_scope)
        self.j_scope = j_scope

    def __enter__(self):
        _push(self.j_scope)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _pop(self.j_scope)
        self.release()

    def release(self):
        """Closes the LivenessScope and releases all the query graph resources.

        Raises:
            DHError
        """
        try:
            self.j_scope.release()
            self.j_scope = None
        except Exception as e:
            raise DHError(e, message="failed to close the LivenessScope.")

    def close(self):
        """Closes the LivenessScope and releases all the query graph resources.

        Raises:
            DHError
        """
        warn('This function is deprecated, prefer release()', DeprecationWarning, stacklevel=2)
        _pop(self.j_scope)
        self.release()

    def open(self, close_after_block: bool = False) -> LivenessScopeFrame:
        """Uses this scope for the duration of the `with` block. The scope will not be
        closed when the block ends."""
        return LivenessScopeFrame(self, close_after_block)

    @property
    def j_object(self) -> jpy.JType:
        return self.j_scope

    def preserve(self, referent: Union[JObjectWrapper, jpy.JType]) -> None:
        """Preserves a query graph node (usually a Table) to keep it live for the outer scope.

        Args:
            referent (Union[JObjectWrapper, jpy.JType]): an object to preserve in the next outer liveness scope

        Raises:
            DHError
        """
        referent = _unwrap_to_liveness_referent(referent)
        try:
            _JLivenessScopeStack.pop(self.j_scope)
            _JLivenessScopeStack.peek().manage(_unwrap_to_liveness_referent(referent))
            _JLivenessScopeStack.push(self.j_scope)
        except Exception as e:
            raise DHError(e, message="failed to preserve a wrapped object in this LivenessScope.")

    def manage(self, referent: Union[JObjectWrapper, jpy.JType]):
        """Explicitly manage the given java object in this scope. Must only be passed a Java LivenessReferent, or
        a Python wrapper around a LivenessReferent"""
        self.j_scope.manage(_unwrap_to_liveness_referent(referent))

    def unmanage(self, referent: Union[JObjectWrapper, jpy.JType]):
        """Explicitly unmanage the given java object from this scope. Must only be passed a Java LivenessReferent, or
        a Python wrapper around a LivenessReferent"""
        self.j_scope.unmanage(_unwrap_to_liveness_referent(referent))


def liveness_scope() -> LivenessScope:
    """Creates a LivenessScope for running a block of code.

    Returns:
        a LivenessScope
    """
    j_scope = _JLivenessScope()
    return LivenessScope(j_scope=j_scope)

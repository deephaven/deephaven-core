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
        return _unwrap_to_liveness_referent(referent.j_object)
    raise DHError("Provided referent isn't a LivenessReferent or a JObjectWrapper around one")


class LivenessScope(JObjectWrapper):
    """A LivenessScope automatically manages reference counting of tables and other query resources that are
    created in it. It implements the context manager protocol and thus can be used in the with statement.

    Note, LivenessScope should not be instantiated directly but rather through the 'liveness_scope' function.
    """
    j_object_type = _JLivenessScope

    def __init__(self, j_scope: jpy.JType):
        self.j_scope = j_scope

    def __enter__(self):
        warn('Instead of passing liveness_scope() to with, call open() on it first',
             DeprecationWarning, stacklevel=2)
        _push(self.j_scope)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _pop(self.j_scope)
        self.release()

    def release(self) -> None:
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
        warn('This function is deprecated, prefer release(). Use cases that rely on this are likely to now fail.',
             DeprecationWarning, stacklevel=2)
        _pop(self.j_scope)
        self.release()

    @contextlib.contextmanager
    def open(self, release_after_block: bool = True) -> "LivenessScope":
        """
        Uses this scope for the duration of the `with` block. The scope defaults to being closed
        when the block ends, disable by passing release_after_block=False

        Args:
            release_after_block: True to release the scope when the block ends, False to leave the
            scope open, allowing it to be reused and keeping collected referents live.  Default is True.

        Returns: None, to allow changes in the future.

        """
        _push(self.j_scope)
        try:
            yield self
        finally:
            _pop(self.j_scope)
            if release_after_block:
                self.release()

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
            # Ensure we are the current scope, throw DHError if we aren't
            _JLivenessScopeStack.pop(self.j_scope)
        except Exception as e:
            raise DHError(e, message="failed to pop the current scope - is preserve() being called on the right scope?")

        try:
            # Manage the object in the next outer scope on this thread.
            _JLivenessScopeStack.peek().manage(_unwrap_to_liveness_referent(referent))
        except Exception as e:
            raise DHError(e, message="failed to preserve a wrapped object in this LivenessScope.")
        finally:
            # Success or failure, restore the scope that was successfully popped
            _JLivenessScopeStack.push(self.j_scope)

    def manage(self, referent: Union[JObjectWrapper, jpy.JType]) -> None:
        """
        Explicitly manage the given java object in this scope. Must only be passed a Java LivenessReferent, or
        a Python wrapper around a LivenessReferent

        Args:
            referent: the object to manage by this scope

        Returns: None

        Raises:
            DHError if the referent isn't a LivenessReferent, or if it is no longer live

        """
        referent = _unwrap_to_liveness_referent(referent)
        try:
            self.j_scope.manage(referent)
        except Exception as e:
            raise DHError(e, message="failed to manage object")

    def unmanage(self, referent: Union[JObjectWrapper, jpy.JType]) -> None:
        """
        Explicitly unmanage the given java object from this scope. Must only be passed a Java LivenessReferent, or
        a Python wrapper around a LivenessReferent

        Args:
            referent: the object to unmanage from this scope

        Returns: None

        Raises:
            DHError if the referent isn't a LivenessReferent, or if it is no longer live

        """
        referent = _unwrap_to_liveness_referent(referent)
        try:
            self.j_scope.unmanage(referent)
        except Exception as e:
            raise DHError(e, message="failed to unmanage object")


def is_liveness_referent(referent: Union[JObjectWrapper, jpy.JType]) -> bool:
    """
    Returns True if the provided object is a LivenessReferent, and so can be managed by a LivenessScope.
    Args:
        referent: the object that maybe a LivenessReferent

    Returns:
        True if the object is a LivenessReferent, False otherwise.
    """
    if isinstance(referent, jpy.JType) and _JLivenessReferent.jclass.isInstance(referent):
        return True
    if isinstance(referent, JObjectWrapper):
        return is_liveness_referent(referent.j_object)
    return False


def liveness_scope() -> LivenessScope:
    """Creates a LivenessScope for running a block of code.

    Returns:
        a LivenessScope
    """
    j_scope = _JLivenessScope()
    return LivenessScope(j_scope=j_scope)

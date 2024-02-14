#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

"""This module gives the users a finer degree of control over when to clean up unreferenced nodes in the query update
graph instead of solely relying on garbage collection.

Examples:

    Use the liveness_scope() function to produce a simple liveness scope that will be used only within a `with` expression
    or as a decorator.

    .. code-block:: python

        with liveness_scope() as scope:
            ticking_table = some_ticking_source()
            table = ticking_table.snapshot().join(table=other_ticking_table, on=...)
            scope.preserve(table)
        return table

    .. code-block:: python

        @liveness_scope
        def get_values():
            ticking_table = some_ticking_source().last_by("Sym")
            return dhnp.to_numpy(ticking_table)


    Use the LivenessScope type for greater control, allowing a scope to be opened more than once, and to release the
    resources that it manages when the scope will no longer be used.


    .. code-block:: python

        def make_table_and_scope(a: int):
            scope = LivenessScope()
            with scope.open():
                ticking_table = some_ticking_source().where(f"A={a}")
                return some_ticking_table, scope

        t1, s1 = make_table_and_scope(1)
        # .. wait for a while
        s1.release()
        t2, s2 = make_table_and_scope(2)
        # etc

    In both cases, the scope object has a few methods that can be used to more directly manage liveness referents:
     * `scope.preserve(obj)` will preserve the given instance in the next scope outside the specified scope instance
     * `scope.manange(obj)` will directly manage the given instance in the specified scope. Take care not to
       double-manage objects when using in conjunction with a `with` block or function decorator.
     * `scope.unmanage(obj)` will stop managing the given instance. This can be used regardless of how the instance
       was managed to begin with.
"""
import contextlib

import jpy

from typing import Union, Iterator
from warnings import warn

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper

_JLivenessScopeStack = jpy.get_type("io.deephaven.engine.liveness.LivenessScopeStack")
_JLivenessScope = jpy.get_type("io.deephaven.engine.liveness.LivenessScope")
_JLivenessReferent = jpy.get_type("io.deephaven.engine.liveness.LivenessReferent")


def _push(scope: _JLivenessScope) -> None:
    _JLivenessScopeStack.push(scope)


def _pop(scope: _JLivenessScope) -> None:
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


class _BaseLivenessScope(JObjectWrapper):
    """
    Internal base type for Java LivenessScope types in python.
    """
    j_object_type = _JLivenessScope

    def __init__(self):
        self.j_scope = _JLivenessScope()

    def _release(self) -> None:
        """Closes the LivenessScope and releases all the query graph resources.

        Raises:
            DHError if this instance has been released too many times
        """
        try:
            self.j_scope.release()
            self.j_scope = None
        except Exception as e:
            raise DHError(e, message="failed to close the LivenessScope.")

    def preserve(self, referent: Union[JObjectWrapper, jpy.JType]) -> None:
        """Preserves a query graph node (usually a Table) to keep it live for the outer scope.

        Args:
            referent (Union[JObjectWrapper, jpy.JType]): an object to preserve in the next outer liveness scope

        Raises:
            DHError if the object isn't a liveness node, or this instance isn't currently at the stop of the stack
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
            referent (Union[JObjectWrapper, jpy.JType]): the object to manage by this scope

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
            referent (Union[JObjectWrapper, jpy.JType]): the object to unmanage from this scope

        Returns: None

        Raises:
            DHError if the referent isn't a LivenessReferent, or if it is no longer live

        """
        referent = _unwrap_to_liveness_referent(referent)
        try:
            self.j_scope.unmanage(referent)
        except Exception as e:
            raise DHError(e, message="failed to unmanage object")

    @property
    def j_object(self) -> jpy.JType:
        return self.j_scope


class SimpleLivenessScope(_BaseLivenessScope):
    """
    A SimpleLivenessScope automatically manages reference counting of tables and other query resources that
    are created in it. Instances are created through calling `liveness_scope()` in a `with` block or as a
    function decorator, and will be disposed of automatically.
    """


class LivenessScope(_BaseLivenessScope):
    """A LivenessScope automatically manages reference counting of tables and other query resources that are
    created in it. Any created instances must have `release()` called on them to correctly free resources
    that have been managed.
    """

    def __init__(self):
        super().__init__()

    def release(self) -> None:
        """Closes the LivenessScope and releases all the managed resources.

        Raises:
            DHError if this instance has been released too many times
        """
        self._release()

    @contextlib.contextmanager
    def open(self) -> Iterator[None]:
        """
        Uses this scope for the duration of a `with` block, automatically managing all resources created in the block.

        Returns: None, to allow changes in the future.
        """
        _push(self.j_scope)
        try:
            yield None
        finally:
            _pop(self.j_scope)


def is_liveness_referent(referent: Union[JObjectWrapper, jpy.JType]) -> bool:
    """
    Returns True if the provided object is a LivenessReferent, and so can be managed by a LivenessScope.
    Args:
        referent: the object that may be a LivenessReferent

    Returns:
        True if the object is a LivenessReferent, False otherwise.
    """
    if isinstance(referent, jpy.JType) and _JLivenessReferent.jclass.isInstance(referent):
        return True
    if isinstance(referent, JObjectWrapper):
        return is_liveness_referent(referent.j_object)
    return False


@contextlib.contextmanager
def liveness_scope() -> Iterator[SimpleLivenessScope]:
    """Creates and opens a LivenessScope for running a block of code. Use
    this function to wrap a block of code using a `with` statement.

    For the duration of the `with` block, the liveness scope will be open
    and any liveness referents created will be manged by it automatically.

    Yields:
        a SimpleLivenessScope
    """
    scope = SimpleLivenessScope()
    _push(scope.j_scope)
    try:
        yield scope
    finally:
        _pop(scope.j_scope)
        scope._release()

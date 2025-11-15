#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
"""This module defines the Barrier marker class and the ConcurrencyControl generic protocol that can be subclassed and
implemented by Selectable and Filter to provide explicit concurrency control during evaluation of the select, update,
and where table operations.


See https://deephaven.io/core/docs/conceptual/query-engine/parallelization/ for more details on concurrency control.

"""

from __future__ import annotations

from abc import abstractmethod
from typing import Protocol, Sequence, TypeVar, Union

import jpy

from deephaven._wrapper import JObjectWrapper

_J_Object = jpy.get_type("java.lang.Object")


class Barrier(JObjectWrapper):
    """A hollow marker class representing a barrier. A barrier imposes an ordering constraint for the filters or
    selectables that respect the same barrier. When a filter/selectable is marked as respecting a barrier object,
    it indicates that the respecting filter/selectable will be executed entirely after the filter/selectable declaring
    the barrier."""

    j_object_type = _J_Object

    @property
    def j_object(self) -> jpy.JType:
        return self.j_barrier

    def __init__(self):  # no parameter so not auto wrap-able
        self.j_barrier = _J_Object()


T = TypeVar("T", covariant=True)


class ConcurrencyControl(Protocol[T]):
    """An abstract class representing concurrency control features for Selectable and Filter."""

    @abstractmethod
    def with_declared_barriers(self, barriers: Union[Barrier, Sequence[Barrier]]) -> T:
        """Returns a new instance with the given declared barriers."""

    @abstractmethod
    def with_respected_barriers(self, barriers: Union[Barrier, Sequence[Barrier]]) -> T:
        """Returns a new instance with the given respected barriers."""

    @abstractmethod
    def with_serial(self) -> T:
        """Returns a new instance with column-wise serial evaluation enforced, i.e. rows in the column are guaranteed to
        evaluated in order."""

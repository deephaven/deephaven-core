#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module defines an Abstract Class for Java object wrappers.

The primary purpose of this ABC is to enable downstream code to retrieve the wrapped Java objects in a uniform way.
"""
from abc import ABC, abstractmethod

import jpy


class JObjectWrapper(ABC):
    @property
    @abstractmethod
    def j_object(self) -> jpy.JType:
        ...
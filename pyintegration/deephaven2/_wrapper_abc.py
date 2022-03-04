#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module defines an Abstract Class for Java object wrappers.

The primary purpose of this ABC is to enable downstream code to retrieve the wrapped Java objects in a uniform way.
"""
from abc import ABC, abstractmethod

import jpy


class JObjectWrapper(ABC):
    def __init_subclass__(cls, *args, **kwargs):
        required_cls_attr = "j_object_type"
        if not hasattr(cls, required_cls_attr):
            raise NotImplementedError(
                f"Class {cls} lacks required `{required_cls_attr}` class attribute"
            )
        if not isinstance(getattr(cls, required_cls_attr), type):
            raise TypeError(f"{required_cls_attr!r} of Class {cls} is not a Class")

    @property
    @abstractmethod
    def j_object(self) -> jpy.JType:
        ...

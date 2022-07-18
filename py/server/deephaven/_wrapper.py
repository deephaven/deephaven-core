#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module defines an Abstract Class for Java object wrappers.

The primary purpose of this ABC is to enable downstream code to retrieve the wrapped Java objects in a uniform way.
"""
from __future__ import annotations

import importlib
import inspect
import pkgutil
import sys
from abc import ABC, abstractmethod
from typing import Set, Union, Optional, Any

import jpy

# a set of all the directly initializable wrapper classes
_di_wrapper_classes: Set[JObjectWrapper] = set()
_has_all_wrappers_imported = False


def _recursive_import(package_path: str) -> None:
    """ Recursively import every module in a package. """

    try:
        pkg = importlib.import_module(package_path)
    except ModuleNotFoundError:
        return

    mods = pkgutil.walk_packages(pkg.__path__)
    for mod in mods:
        mod_path = ".".join([package_path, mod.name])
        if mod.ispkg:
            _recursive_import(mod_path)
        else:
            if mod_path not in sys.modules:
                try:
                    importlib.import_module(mod_path)
                except:
                    ...


class JObjectWrapper(ABC):
    j_object_type: type

    def __init_subclass__(cls, *args, **kwargs):
        required_cls_attr = "j_object_type"
        if not hasattr(cls, required_cls_attr):
            raise NotImplementedError(f"Class {cls} lacks required `{required_cls_attr}` class attribute")
        if not isinstance(getattr(cls, required_cls_attr), type):
            raise TypeError(f"{required_cls_attr!r} of Class {cls} is not a Class")

        if _is_direct_initialisable(cls):
            _di_wrapper_classes.add(cls)

    @property
    @abstractmethod
    def j_object(self) -> jpy.JType:
        ...

    def __repr__(self):
        self_type = type(self)
        return f"{self_type.__module__}.{self_type.__qualname__}({repr(self.j_object)})"

    def __str__(self):
        return str(self.j_object)

    def __hash__(self):
        return hash(self.j_object)

    def __eq__(self, other):
        return self.j_object == other.j_object if isinstance(other, self.__class__) else NotImplemented

    def __ne__(self, other):
        return self.j_object != other.j_object if isinstance(other, self.__class__) else NotImplemented

    def __lt__(self, other):
        return self.j_object < other.j_object if isinstance(other, self.__class__) else NotImplemented

    def __le__(self, other):
        return self.j_object <= other.j_object if isinstance(other, self.__class__) else NotImplemented

    def __gt__(self, other):
        return self.j_object > other.j_object if isinstance(other, self.__class__) else NotImplemented

    def __ge__(self, other):
        return self.j_object >= other.j_object if isinstance(other, self.__class__) else NotImplemented


def _is_direct_initialisable(cls) -> bool:
    """ Returns whether a wrapper class instance can be initialized with a Java object. """
    funcs = inspect.getmembers(cls, inspect.isfunction)
    init_funcs = [func for name, func in funcs if name == "__init__"]
    if init_funcs:
        init_func = init_funcs[0]
        sig = inspect.signature(init_func)
        if len(sig.parameters) == 2:
            _, param_meta = list(sig.parameters.items())[1]
            if param_meta.annotation == 'jpy.JType' or param_meta.annotation == jpy.JType:
                return True

    return False


def _lookup_wrapped_class(j_obj: jpy.JType) -> Optional[type]:
    """ Returns the wrapper class for the specified Java object. """
    # load every module in the deephaven package so that all the wrapper classes are loaded and available to wrap
    # the Java objects returned by calling resolve()
    global _has_all_wrappers_imported
    if not _has_all_wrappers_imported:
        _recursive_import(__package__.partition(".")[0])
        _has_all_wrappers_imported = True

    for wc in _di_wrapper_classes:
        j_clz = wc.j_object_type
        if j_clz.jclass.isInstance(j_obj):
            return wc

    return None


def wrap_j_object(j_obj: jpy.JType) -> Union[JObjectWrapper, jpy.JType]:
    """ Wraps the specified Java object as an instance of a custom wrapper class if one is available, otherwise returns
    the raw Java object. """
    if j_obj is None:
        return None

    wc = _lookup_wrapped_class(j_obj)

    return wc(j_obj) if wc else j_obj


def unwrap(obj: Any) -> Union[jpy.JType, Any]:
    """ Returns the wrapped raw Java object if this is a wrapped Java object. Otherwise, returns the same object. """
    if isinstance(obj, JObjectWrapper):
        return obj.j_object

    return obj

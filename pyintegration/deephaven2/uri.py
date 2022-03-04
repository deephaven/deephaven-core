#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" Tools for resolving URIs into objects including Tables. """

import inspect
from typing import Any, Union

import jpy

from deephaven2 import DHError
from deephaven2._wrapper_abc import JObjectWrapper

_JResolveTools = jpy.get_type("io.deephaven.uri.ResolveTools")


def _is_direct_initialisable(cls):
    funcs = inspect.getmembers(cls, inspect.isfunction)
    init_funcs = [func for name, func in funcs if name == "__init__"]
    if init_funcs:
        init_func = init_funcs[0]
        sig = inspect.signature(init_func)
        if len(sig.parameters) == 2:
            _, param_meta = list(sig.parameters.items())[1]
            if param_meta.annotation == "jpy.JType":
                return True

    return False


def _find_deephaven_wrappers():
    """Returns a set of Python classes that wrap the Deephaven Java ones and must be
    directly initialisable."""
    import deephaven2

    modules = inspect.getmembers(deephaven2, inspect.ismodule)
    wrapper_cls_set = set()
    for _, m in modules:
        classes = inspect.getmembers(m, inspect.isclass)
        wrappers = [
            cls
            for _, cls in classes
            if issubclass(cls, JObjectWrapper) and cls is not JObjectWrapper
        ]
        wrapper_cls_set.update(wrappers)

    return {wc for wc in wrapper_cls_set if _is_direct_initialisable(wc)}


_wrapped_cls_dict = {wc.j_object_type: wc for wc in _find_deephaven_wrappers()}


def _lookup_wrapped_class(j_obj: jpy.JType) -> Any:
    for j_clz, wc in _wrapped_cls_dict.items():
        if j_clz.jclass.isInstance(j_obj):
            return wc

    return None


def _wrap_resolved_object(j_obj: jpy.JType) -> Any:
    if j_obj is None:
        return None

    wc = _lookup_wrapped_class(j_obj)

    return wc(j_obj) if wc else j_obj


def resolve(uri: str) -> Union[jpy.JType, JObjectWrapper]:
    """Resolves the uri string into a Java object. If the Java object can be wrapped with a custom Python wrapper
    such as Table, this function returns an instance of the wrapper class.


    Args:
        uri (str): a uri string

    Returns:
        an object

    Raises:
        DHError
    """
    try:
        return _wrap_resolved_object(_JResolveTools.resolve(uri))
    except Exception as e:
        raise DHError(e, "failed to resolve the uri.") from e

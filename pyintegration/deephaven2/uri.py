#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" Tools for resolving URIs into TODO """

import inspect
from typing import Any

import jpy

from deephaven2 import DHError
from deephaven2._wrapper_abc import JObjectWrapper

_JResolveTools = jpy.get_type("io.deephaven.uri.ResolveTools")


def get_deephaven_wrapper_types():
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

    return {wc.j_object_type(): wc for wc in wrapper_cls_set}


wrapped_cls_dict = get_deephaven_wrapper_types()


def wrap_resolved_object(j_obj: jpy.JType) -> Any:
    if j_obj is None:
        return None

    wc = wrapped_cls_dict.get(type(j_obj), None)
    if not wc:
        return j_obj
    else:
        try:
            w_obj = wc(j_obj)
            return w_obj
        except:
            return j_obj


def resolve(uri: str):
    """Resolve the uri string into an object
    TODO 1. clarify with Devin the possible types and the need to wrap?; 2. how to test


    Args:
        uri (str): a uri string

    Returns:
        an object

    Raises:
        DHError
    """
    try:
        return wrap_resolved_object(_JResolveTools.resolve(uri))
    except Exception as e:
        raise DHError(e, "failed to resolve the uri.") from e

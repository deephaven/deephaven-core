#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" Tools for resolving Uniform Resource Identifiers (URIs) into objects. """

from typing import Any

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper, wrap_j_object

_JResolveTools = jpy.get_type("io.deephaven.uri.ResolveTools")


def resolve(uri: str) -> Any:
    """Resolves a Uniform Resource Identifier (URI) string into an object. Objects with custom Python wrappers,
    like Table, return an instance of the wrapper class; otherwise, the Java or Python object is returned.


    Args:
        uri (str): a URI string

    Returns:
        an object

    Raises:
        DHError
    """

    try:
        # When grabbing something out of the query scope, it may already be presented as a PyObject; in which case,
        # when the value gets back into python, it's a native python type - in which case, we don't need to wrap it.
        item = _JResolveTools.resolve(uri)
        return wrap_j_object(item) if isinstance(item, jpy.JType) else item
    except Exception as e:
        raise DHError(e, "failed to resolve the URI.") from e

#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" Tools for resolving Uniform Resource Identifiers (URIs) into objects. """

from typing import Union

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper, wrap_j_object

_JResolveTools = jpy.get_type("io.deephaven.uri.ResolveTools")


def resolve(uri: str) -> Union[jpy.JType, JObjectWrapper]:
    """Resolves a Uniform Resource Identifier (URI) string into an object. Objects with custom Python wrappers,
    like Table, return an instance of the wrapper class; otherwise, the raw Java object is returned.


    Args:
        uri (str): a URI string

    Returns:
        an object

    Raises:
        DHError
    """

    try:
        return wrap_j_object(_JResolveTools.resolve(uri))
    except Exception as e:
        raise DHError(e, "failed to resolve the URI.") from e

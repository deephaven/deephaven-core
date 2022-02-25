#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" Tools for resolving URIs into TODO """

import jpy

from deephaven2 import DHError

_JResolveTools = jpy.get_type("io.deephaven.uri.ResolveTools")


def resolve(uri: str):
    """ Resolve the uri string into an object
    TODO 1. clarify with Devin the possible types and the need to wrap?; 2. how to test


    Args:
        uri (str): a uri string

    Returns:
        an object

    Raises:
        DHError
    """
    try:
        return _JResolveTools.resolve(uri)
    except Exception as e:
        raise DHError(e, "failed to resolve the uri.") from e

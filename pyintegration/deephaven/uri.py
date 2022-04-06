#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" Tools for resolving Uniform Resource Identifiers (URIs) into objects. """

import importlib
import pkgutil
import sys
from typing import Union

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper, wrap_j_object

_JResolveTools = jpy.get_type("io.deephaven.uri.ResolveTools")


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


# load every module in the deephaven package so that all the wrapper classes are loaded and available to wrap
# the Java objects returned by calling resolve()
_recursive_import(__package__.partition(".")[0])


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

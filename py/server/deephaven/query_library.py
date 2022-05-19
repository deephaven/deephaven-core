#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module allows users to import Java classes or packages into the query library for the Deephaven query engine.
These classes or packages then can be used in Deephaven queries. """
from typing import List

import jpy
from deephaven import DHError

_JPackage = jpy.get_type("java.lang.Package")
_JQueryLibrary = jpy.get_type("io.deephaven.engine.table.lang.QueryLibrary")
_JClass = jpy.get_type("java.lang.Class")


def import_class(name: str) -> None:
    """Imports a Java class into the Query library so that it can be referenced in Deephaven queries. The class must
    be reachable in the Deephaven server's classpath.

    Args:
        name (str): the full qualified name of the Java class

    Raises:
        DHError
    """
    try:
        j_class = _JClass.forName(name)
        _JQueryLibrary.importClass(j_class)
    except Exception as e:
        raise DHError(e, "failed to add the Java class to the Query Library.") from e


def import_static(name: str) -> None:
    """Imports the static members of a Java class into the Query library so that they can be referenced in Deephaven
    queries. The class must be reachable in the Deephaven server's classpath.

    Args:
        name (str): the full qualified name of the Java class

    Raises:
        DHError
    """
    try:
        j_class = _JClass.forName(name)
        _JQueryLibrary.importStatic(j_class)
    except Exception as e:
        raise DHError(e, "failed to import the static members of the Java class to the Query Library.") from e


def import_package(name: str) -> None:
    """Imports a Java package into the Query library so that it can be referenced in Deephaven queries. The package
    must be reachable in the Deephaven server's classpath.

    Args:
        name (str): the full qualified name of the Java package

    Raises:
        DHError
    """
    try:
        j_package = _JPackage.getPackage(name)
        _JQueryLibrary.importPackage(j_package)
    except Exception as e:
        raise DHError(e, "failed to import the Java package into to the Query Library.") from e


def imports() -> List[str]:
    """Returns all the Java import statements currently in the Query Library.

    Returns:
        a list of strings
    """
    return list(_JQueryLibrary.getImportStrings().toArray())[1:]

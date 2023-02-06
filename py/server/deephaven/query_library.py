#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

"""This module allows users to import Java classes or packages into the query library for the Deephaven query engine.
These classes or packages can then be used in Deephaven queries. """
from typing import List

import jpy
from deephaven import DHError

_JPackage = jpy.get_type("java.lang.Package")
_JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
_JClass = jpy.get_type("java.lang.Class")


def import_class(name: str) -> None:
    """Adds a Java class to the query library, making it available to be used in Deephaven query strings (formulas
    and conditional expressions). The class must be reachable in the Deephaven server's classpath.

    Args:
        name (str): the fully qualified name of the Java class

    Raises:
        DHError
    """
    try:
        j_class = _JClass.forName(name)
        _JExecutionContext.getContext().getQueryLibrary().importClass(j_class)
    except Exception as e:
        raise DHError(e, "failed to add the Java class to the Query Library.") from e


def import_static(name: str) -> None:
    """Adds the static members of a Java class to the query library, making them available to be used in Deephaven
    query strings (formulas and conditional expressions). The class must be reachable in the Deephaven server's
    classpath.

    Args:
        name (str): the fully qualified name of the Java class

    Raises:
        DHError
    """
    try:
        j_class = _JClass.forName(name)
        _JExecutionContext.getContext().getQueryLibrary().importStatic(j_class)
    except Exception as e:
        raise DHError(e, "failed to add the static members of the Java class to the Query Library.") from e


def import_package(name: str) -> None:
    """Adds all the public classes and interfaces of a Java package to the query library, making them available to be
    used in Deephaven query strings (formulas and conditional expressions). The package must be reachable in the
    Deephaven server's classpath.

    Args:
        name (str): the fully qualified name of the Java package

    Raises:
        DHError
    """
    try:
        j_package = _JPackage.getPackage(name)
        _JExecutionContext.getContext().getQueryLibrary().importPackage(j_package)
    except Exception as e:
        raise DHError(e, "failed to add the Java package into to the Query Library.") from e


def imports() -> List[str]:
    """Returns all the Java import statements currently in the Query Library.

    Returns:
        a list of strings
    """
    return list(_JExecutionContext.getContext().getQueryLibrary().getImportStrings().toArray())[1:]

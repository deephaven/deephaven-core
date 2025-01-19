#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

""" This module supports bootstrapping a Deephaven Python Script session from Python."""

import os
import re
import warnings
from glob import glob
from typing import Dict

from deephaven_internal import jvm

py_dh_session = None

def start_jvm_for_tests(jvm_props: Dict[str, str] = None):
    jvm.preload_jvm_dll()
    import jpy

    """ This function uses the default DH property file to embed the Deephaven server and starts a Deephaven Python
    Script session. """
    if not jpy.has_jvm():

        # we will try to initialize the jvm
        propfile = os.environ.get('DEEPHAVEN_PROPFILE', 'dh-defaults.prop')

        jvm_properties = {
            'PyObject.cleanup_on_thread': 'false',

            'java.awt.headless': 'true',
            'MetricsManager.enabled': 'true',

            'Configuration.rootFile': propfile,
            'deephaven.dataDir': '/data',
            'deephaven.cacheDir': '/cache',

            'Calendar.default': 'USNYSE_EXAMPLE',
            'Calendar.importPath': '/test_calendar_imports.txt',
            'SystemicObjectTracker.enabled': 'true',
        }

        if jvm_props:
            jvm_properties.update(jvm_props)

        jvm_options = {
            # Allow access to java.nio.Buffer fields
            '--add-opens=java.base/java.nio=ALL-UNNAMED',

            # Allow our hotspot-impl project to access internals
            '--add-exports=java.management/sun.management=ALL-UNNAMED',

            # Allow our clock-impl project to access internals
            '--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED',
        }
        jvm_classpath = os.environ.get('DEEPHAVEN_CLASSPATH', '')

        # Intentionally small by default - callers should set as appropriate
        jvm_maxmem = os.environ.get('DEEPHAVEN_MAXMEM', '256m')

        # Start up the JVM
        jpy.VerboseExceptions.enabled = True
        jvm.init_jvm(
            jvm_maxmem=jvm_maxmem,
            jvm_classpath=_expand_wildcards_in_list(jvm_classpath.split(os.path.pathsep)),
            jvm_properties=jvm_properties,
            jvm_options=jvm_options
        )

        # Set up a Deephaven Python session
        py_scope_jpy = jpy.get_type("io.deephaven.engine.util.PythonScopeJpyImpl").ofMainGlobals()
        global py_dh_session

        no_op_thread_factory = jpy.get_type("io.deephaven.util.thread.ThreadInitializationFactory").NO_OP
        _JOperationInitializationThreadPool = jpy.get_type("io.deephaven.engine.table.impl.OperationInitializationThreadPool")
        _j_operation_initializer = _JOperationInitializationThreadPool(no_op_thread_factory)

        _JPeriodicUpdateGraph = jpy.get_type("io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph")
        _j_test_update_graph = _JPeriodicUpdateGraph.newBuilder(_JPeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME) \
                .operationInitializer(_j_operation_initializer) \
                .existingOrBuild()

        _JPythonScriptSession = jpy.get_type("io.deephaven.integrations.python.PythonDeephavenSession")
        py_dh_session = _JPythonScriptSession(_j_test_update_graph, _j_operation_initializer, no_op_thread_factory, py_scope_jpy)


def _expand_wildcards_in_list(elements):
    """
    Takes list of strings, possibly containing wildcard characters, and returns the corresponding full list. This is
    intended for appropriately expanding classpath entries.

    :param elements: list of strings (paths)
    :return: corresponding list of expanded paths
    """

    new_list = []
    for element in elements:
        new_list.extend(_expand_wildcards_in_item(element))
    return _flatten(new_list)


def _expand_wildcards_in_item(element):
    """
    Java classpaths can include wildcards (``<path>/*`` or ``<path>/*.jar``), but the way we are invoking the jvm
    directly bypasses this expansion. This will expand a classpath element into an array of elements.

    :return: an array of all the jars matching the input wildcard, or the original string if it isn't a wildcard
    """

    if not element.endswith(("/*", "/*.jar", os.path.sep + "*", os.path.sep + "*.jar")):
        return [element]

    # extract the base - everything up to the last separator (always accept /) followed by * or *.jar
    # (group 0 = anything)[slash or path.sep]star(group 1 optional .jar)
    # backslashes in regular expressions are problematic, so convert the element to / delimiters
    try:
        base = re.search("(.*)/\*(.jar)?$", element.replace("\\", "/")).group(1)
        # expand base
        return glob("{}/*.jar".format(base))
    except AttributeError:
        return [element, ]


def _flatten(orig):
    """
    Converts the contents of list containing strings, lists of strings *(,lists of lists of strings,...)* to a flat
    list of strings.

    :param orig: the list to flatten
    :return: the flattened list
    """

    if isinstance(orig, str):
        return [orig, ]
    elif not hasattr(orig, '__iter__'):
        raise ValueError("The flatten method only accepts string or iterable input")

    out = []
    for x in orig:
        if isinstance(x, str):
            out.append(x)
        elif hasattr(x, '__iter__'):
            out.extend(_flatten(x))
        else:
            raise ValueError("The flatten method encountered an invalid entry of type {}".format(type(x)))
    return out

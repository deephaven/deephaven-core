#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module supports bootstrapping a Deephaven Python Script session from Python."""

import os
import re
import warnings
from glob import glob
from typing import Dict

from deephaven_internal import jvm

def start_jvm(jvm_props: Dict[str, str] = None):
    jvm.preload_jvm_dll()
    import jpy

    """ This function uses the default DH property file to embed the Deephaven server and starts a Deephaven Python
    Script session. """
    if not jpy.has_jvm():

        # we will try to initialize the jvm
        workspace = os.environ.get('DEEPHAVEN_WORKSPACE', '.')
        devroot = os.environ.get('DEEPHAVEN_DEVROOT', '.')
        propfile = os.environ.get('DEEPHAVEN_PROPFILE', 'dh-defaults.prop')

        # validate devroot & workspace
        if devroot is None:
            raise IOError("dh.init: devroot is not specified.")
        if not os.path.isdir(devroot):
            raise IOError("dh.init: devroot={} does not exist.".format(devroot))

        if workspace is None:
            raise IOError("dh.init: workspace is not specified.")
        if not os.path.isdir(workspace):
            raise IOError("dh.init: workspace={} does not exist.".format(workspace))

        dtemp = workspace
        for entry in ['', 'cache', 'classes']:
            dtemp = os.path.join(dtemp, entry)
            if os.path.exists(dtemp):
                if not (os.path.isdir(dtemp) and os.access(dtemp, os.W_OK | os.X_OK)):
                    # this is silly, but a directory must be both writable and executible by a user for a
                    # file to be written there - write without executible is delete only
                    raise IOError("dh.init: workspace directory={} does exists, but is "
                                  "not writeable by your user.".format(dtemp))
            else:
                # Log potentially helpful warning - in case of failure.
                warnings.warn("dh.init: workspace directory={} does not exist, and its absence may "
                              "lead to an error. When required, it SHOULD get created with appropriate "
                              "permissions by the Deephaven class DynamicCompileUtils. If strange errors arise "
                              "from jpy about inability to find some java class, then check "
                              "the existence/permissions of the directory.".format(dtemp),
                              RuntimeWarning)

        jvm_properties = {
            'PyObject.cleanup_on_thread': 'false',

            'java.awt.headless': 'true',
            'MetricsManager.enabled': 'true',

            'Configuration.rootFile': propfile,
            'devroot': os.path.realpath(devroot),
            'workspace': os.path.realpath(workspace),

        }

        if jvm_props:
            jvm_properties.update(jvm_props)

        jvm_options = {
            '-XX:+UseG1GC',
            '-XX:MaxGCPauseMillis=100',
            '-XX:+UseStringDeduplication',

            '-XX:InitialRAMPercentage=25.0',
            '-XX:MinRAMPercentage=70.0',
            '-XX:MaxRAMPercentage=80.0',

            '--add-opens=java.base/java.nio=ALL-UNNAMED',
        }
        jvm_classpath = os.environ.get('DEEPHAVEN_CLASSPATH', '')


        # Start up the JVM
        jpy.VerboseExceptions.enabled = True
        jvm.init_jvm(
            jvm_classpath=_expandWildcardsInList(jvm_classpath.split(os.path.pathsep)),
            jvm_properties=jvm_properties,
            jvm_options=jvm_options
        )

        # Set up a Deephaven Python session
        py_scope_jpy = jpy.get_type("io.deephaven.engine.util.PythonScopeJpyImpl").ofMainGlobals()
        py_dh_session = jpy.get_type("io.deephaven.integrations.python.PythonDeephavenSession")(py_scope_jpy)
        py_dh_session.getExecutionContext().open()


def _expandWildcardsInList(elements):
    """
    Takes list of strings, possibly containing wildcard characters, and returns the corresponding full list. This is
    intended for appropriately expanding classpath entries.

    :param elements: list of strings (paths)
    :return: corresponding list of expanded paths
    """

    new_list = []
    for element in elements:
        new_list.extend(_expandWildcardsInItem(element))
    return _flatten(new_list)


def _expandWildcardsInItem(element):
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

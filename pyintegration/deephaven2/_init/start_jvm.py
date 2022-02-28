#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" Utilities for starting the Deephaven JVM. """

import os
import re
import warnings
from glob import glob
from os import path

import jpy
import jpyutil


# from deephaven import initialize
# from .conversion_utils import _isStr


def start_jvm(devroot=None,
              workspace=None,
              propfile=None,
              keyfile=None,
              verbose=False,
              skip_default_classpath=None,
              # The following are the jpyutil.init_jvm options which are passed through after attaching our options
              java_home=None,
              jvm_dll=None,
              jvm_maxmem=None,
              jvm_classpath=None,
              jvm_properties=None,
              jvm_options=None,
              config_file=None,
              config=None):
    """
    Starts a JVM within this Python process to interface with Deephaven.

    This is a small convenience wrapper around :func:`jpyutil.init_jvm`. Additionally, the Configuration is loaded
    and and Deephaven classes are brought into Python.

    :param devroot: the devroot parameter for Deephaven. Defaults to the ``ILLUMON_DEVROOT`` environment variable, or
      ``/usr/deephaven/latest``
    :param workspace: the workspace parameter for Deephaven. Defaults to the ``ILLUMON_WORKSPACE`` environment variable
    :param propfile: the ``Configuration.rootFile`` parameter for Deephaven. Defaults to the ``ILLUMON_PROPFILE`` environment
      variable
    :param keyfile: your private key file for authenticating to Deephaven
    :param skip_default_classpath: if True, do not attempt to compute default java classpath
    :param verbose: if True, print out the classpath and properties we have constructed

    The rest of the parameters are passed through to :func:`jpyutil.init_jvm`. The values for `jvm_classpath` and
    `jvm_properties` may have been modified based on the values of other arguments.

    :param java_home: The Java JRE or JDK home directory used to search JVM shared library, if 'jvm_dll' is omitted.
    :param jvm_dll: The JVM shared library file. My be inferred from 'java_home'.
    :param jvm_maxmem: The JVM maximum heap space, e.g. '400M', '8G'. Refer to the java executable '-Xmx' option.
    :param jvm_classpath: optional initial classpath elements. Default elements will be appended unless
      `skip_default_classpath` is specified
    :param jvm_properties: inserted into the dictionary generated by `devroot`, `workspace`, `propfile`, and `keyfile`.
    :param jvm_options: A list of extra options for the JVM. Refer to the java executable options.
    :param config_file: Extra configuration file (e.g. 'jpyconfig.py') to be loaded if 'config' parameter is omitted.
    :param config: An optional default configuration object providing default attributes
                   for the 'jvm_maxmem', 'jvm_classpath', 'jvm_properties', 'jvm_options' parameters.
    """

    # setup defaults

    for stem in ['DEEPHAVEN']:
        if devroot is None:
            devroot = os.environ.get("{}_DEVROOT".format(stem), None)
        if workspace is None:
            workspace = os.environ.get("{}_WORKSPACE".format(stem), None)
        if propfile is None:
            propfile = os.environ.get("{}_PROPFILE".format(stem), None)

    # if we don't have a devroot and/or propfile yet, workers will have standard environment variables we can try
    if devroot is None:
        devroot = os.environ.get('DEVROOT', None)

    if propfile is None:
        propfile = os.environ.get('CONFIGFILE', None)

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

    # setup environment
    expanded_devroot = _expandLinks(devroot)

    jProperties = {'devroot': expanded_devroot, 'workspace': workspace}
    if propfile is not None:
        jProperties['Configuration.rootFile'] = propfile
    if keyfile is not None:
        jProperties['WAuthenticationClientManager.defaultPrivateKeyFile'] = keyfile
    if jvm_properties is not None:
        jProperties.update(jvm_properties)

    jClassPath = []
    # allow for string or array, because users get confused
    if jvm_classpath is not None:
        if isinstance(jvm_classpath, str):
            jClassPath.extend(jvm_classpath.split(os.path.pathsep))
        elif isinstance(jvm_classpath, list):
            jClassPath.extend(jvm_classpath)
        else:
            raise ValueError("Invalid jvm_classpath type = {}. list or string accepted.".format(type(jvm_classpath)))

    defaultClasspath = None
    if not skip_default_classpath:
        defaultClasspath = _getDefaultClasspath(expanded_devroot, workspace)
        jClassPath.extend(defaultClasspath)

    jClassPath = _expandWildcardsInList(jClassPath)

    if verbose:
        print("JVM default classpath... {}".format(defaultClasspath))
        print("JVM classpath... {}".format(jClassPath))
        print("JVM properties... {}".format(jProperties))

    if jvm_options is None:
        jvm_options=set()

    if path.exists("/usr/deephaven/latest/etc/JAVA_VERSION"):
        java_version_file=open("/usr/deephaven/latest/etc/JAVA_VERSION", "r")
        java_version=java_version_file.read()
        java_version_file.close()
    elif path.exists("{}/props/configs/build/resources/main/JAVA_VERSION".format(devroot)):
        java_version_file=open("{}/props/configs/build/resources/main/JAVA_VERSION".format(devroot), "r")
        java_version=java_version_file.read()
        java_version_file.close()
    elif os.environ.get('JAVA_VERSION') is not None:
        java_version=os.environ.get('JAVA_VERSION')
    else:
        raise ValueError('Cannot find JAVA_VERSION from environment variable or filesystem locations')


    if not any(elem.startswith('--add-opens') for elem in jvm_options):
        if java_version == '11':
            jvm_options.add('--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED')
        elif java_version == '13':
            jvm_options.add('--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED')
            jvm_options.add('--add-opens=java.base/jdk.internal.access=ALL-UNNAMED')

    if verbose:
        if len(jvm_options) > 0:
            print("JVM options... {}".format(jvm_options))

    jpy.VerboseExceptions.enabled = True
    jpyutil.init_jvm(
        java_home=java_home,
        jvm_dll=jvm_dll,
        jvm_maxmem=jvm_maxmem,
        jvm_classpath=jClassPath,
        jvm_properties=jProperties,
        jvm_options=jvm_options,
        config_file=config_file,
        config=config)
    # Loads our configuration and initializes the class types
    # TODO not sure if this is still needed.
    # initialize()


def _expandLinks(dirname):
    return os.path.realpath(dirname)

def _getDefaultClasspath(devroot, workspace):
    """
    Determines whether running as client or server, and returns default classpath elements accordingly.

    :param devroot: the devroot parameter for Deephaven; will be set by the time this is called
    :param workspace: the workspace parameter for Deephaven; will be set by the time this is called
    :return: the default classpath as an array of strings
    """

    # first determine whether this is client or server
    # clients have devroot/getdown.txt
    # servers have devroot, typically the link /usr/deephaven/latest expanded to an actual directory
    # if neither seems to apply, fail

    isclient = None
    if os.path.isfile(os.path.join(devroot, "getdown.txt")):
        isclient = True
    else:
        if os.path.isdir(devroot):
            isclient = False
        else:
            raise IOError("Could not decide how to create classpath. Neither {} nor "
                          "<devroot>/getdown.txt exist".format(devroot))

    if isclient:
        # this construction should match the classpath specified in getdown.txt
        return _flatten([os.path.join(devroot, el) for el in ["private_classes", "private_jars/*", "override",
                                                             "resources", "hotfixes/*", "java_lib/*"]])
    else:  # is server
        # this construction should match the classpath specified in launch and launch_functions
        return _flatten(["{}/etc".format(workspace),
                        "/etc/sysconfig/deephaven.d/override",
                        "/etc/sysconfig/deephaven.d/resources",
                        "/etc/sysconfig/deephaven.d/java_lib/*",
                        "/etc/sysconfig/deephaven.d/hotfixes/*",
                         _addPluginClasspaths(),
                        "{}/etc".format(devroot),
                        "{}/java_lib/*".format(devroot)])


def _addPluginClasspaths():
    """
    Helper method. Adds elements to classpath listing.

    :return: classpath fragment
    """

    new_list = []
    _addGlobal(new_list, "/etc/sysconfig/deephaven.d/plugins")
    _addGlobal(new_list, "/etc/deephaven/plugins")
    return new_list


def _addGlobal(new_list, base):
    """
    Helper method. Add elements to classpath listing. Resolves links to actual directories.

    :param new_list: current classpath list
    :param base: base directory
    """

    if not os.path.isdir(base):
        return
    for plugin in os.listdir(base):
        thedir = os.path.join(base, plugin, "global")
        if os.path.isdir(thedir):
            expanded_plugin_dir = _expandLinks(thedir)
            new_list.extend([os.path.join(expanded_plugin_dir, dependency) for dependency in os.listdir(expanded_plugin_dir)])


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

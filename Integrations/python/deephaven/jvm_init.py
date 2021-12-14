#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""Utilities for initializing the Deephaven JVM."""

import jpy
import jpyutil
import logging
import os
from glob import glob

from deephaven import initialize


def jvm_init(devroot=None,
             workspace=None,
             propfile=None,
             userHome=None,
             keyfile=None,
             librarypath=None,
             workerHeapGB=12,
             jvmHeapGB=2,
             jvmOptions=None,
             verbose=False):
    """
    Initialize the JVM to run Deephaven.

    :param devroot: for Deephaven Java installation - must include trailing path separator
    :param workspace: Java workspace directory
    :param propfile: Deephaven Java propfile
    :param userHome: User's home directory
    :param keyfile: path to private key file for Deephaven user authentication
    :param librarypath: Java library path
    :param workerHeapGB: desired worker heap in GB
    :param jvmHeapGB: Desired jvm heap in GB
    :param jvmOptions: optional jvm options
    :param verbose: enable / disable verbose output

    .. note:: for clarity, the `userHome` parameter should be provided. If not, it resolves to the home directory of
        user determined by environment variable ``USER`` or ``USERNAME``. If neither of those is set, it resolves to
        the home directory of ``~``. These resolutions are performed using :func:`os.path.expanduser`.
    """

    # setup defaults
    for stem in ['DEEPHAVEN', 'ILLUMON']:
        if devroot is None:
            devroot = os.environ.get("{}_DEVROOT".format(stem), None)
        if workspace is None:
            workspace = os.environ.get("{}_WORKSPACE".format(stem), None)
        if propfile is None:
            propfile = os.environ.get("{}_PROPFILE".format(stem), None)

    if not os.path.isdir(devroot):
        raise Exception("dh.init: devroot={} does not exist.".format(devroot))
    if not os.path.isdir(workspace):
        os.makedirs(workspace)
        if verbose:
            print("dh.init: Creating workspace folder {}".format(workspace))

    username = None
    if userHome is None:
        for val in ['USER', 'USERNAME']:
            if username is None:
                username = os.environ.get(val, None)
        if username is not None:
            userHome = os.path.expanduser('~'+username)
        else:
            userHome = os.path.expanduser('~')

        if userHome[0] == '~':
            # NB: this expansion could fail, and will just leave these unchanged...what would I do?
            userHome = None
    if (userHome is not None) and (userHome[-1] != '/'):
        userHome += "/"
    if verbose:
        print("dh.init: userHome = {}".format(userHome))

    if keyfile is None and username is not None:
        keyfile1 = os.path.join(userHome, 'priv-{}.base64.txt'.format(username))
        if os.path.exists(keyfile1):
            keyfile = keyfile1
    if not os.path.isfile(keyfile):
        raise Exception("dh.init: keyfile={} does not exist.".format(keyfile))

    # setup environment
    jProperties = {
        'workspace': workspace,
        'Configuration.rootFile': propfile,
        'devroot': devroot,
        'disable.jvmstatus': 'true',
        'RemoteProcessingRequest.defaultQueryHeapMB': '{}'.format(workerHeapGB * 1024),
        'useLongClientDelayThreshold': 'true',
        'WAuthenticationClientManager.defaultPrivateKeyFile': keyfile
    }

    if librarypath is not None:
        jProperties['java.library.path'] = librarypath

    if verbose:
        print("JVM properties...\n{}".format(jProperties))

    jClassPath = [os.path.join(devroot, el) for el in ['etc', 'Common/config', 'configs',
                                                       'build/classes/main', 'build/resources/main']]

    jClassPath.extend(glob("{}/*/build/classes/main".format(devroot)))

    for root, dirs, files in os.walk(os.path.join(devroot, 'lib')):
        for f in files:
            if f.endswith(".jar"):
                fname = os.path.join(root, f)
                logging.info("JAR {}".format(fname))
                jClassPath.append(fname)

    for root, dirs, files in os.walk(os.path.join(devroot, "build", "jars")):
        for f in files:
            if f.endswith(".jar"):
                fname = os.path.join(root, f)
                logging.info("JAR {}".format(fname))
                jClassPath.append(fname)

    if verbose:
        print("JVM classpath...{}".format(jClassPath))

    jpy.VerboseExceptions.enabled = True

    jvm_options = []
    if jvmOptions is None:
        pass
    elif isinstance(jvmOptions, str):
        jvm_options = jvmOptions.strip().split()
    elif isinstance(jvmOptions, list):
        jvm_options = jvmOptions
    if verbose and len(jvm_options) > 0:
        print("JVM Options...{}".format(jvm_options))

    jpyutil.init_jvm(jvm_maxmem='{}m'.format(jvmHeapGB*1024), jvm_properties=jProperties,
                     jvm_classpath=jClassPath, jvm_options=jvm_options)
    # Loads our configuration and initializes the class types
    initialize()

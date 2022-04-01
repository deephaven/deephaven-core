#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module supports bootstrapping a Deephaven Python Script session from Python."""

import os

import jpy

from .start_jvm import start_jvm

DEFAULT_DEVROOT = os.environ.get('DEEPHAVEN_DEVROOT', "/tmp/pyintegration")
DEFAULT_WORKSPACE = os.environ.get('DEEPHAVEN_WORKSPACE', "/tmp")
DEFAULT_PROPFILE = os.environ.get('DEEPHAVEN_PROPFILE', 'dh-defaults.prop')
DEFAULT_CLASSPATH = os.environ.get('DEEPHAVEN_CLASSPATH', "/opt/deephaven/server/lib/*")


def build_py_session():
    """ This function uses the default DH property file to embed the Deephaven server and starts a Deephaven Python
    Script session. """
    if not jpy.has_jvm():
        os.environ['JAVA_VERSION'] = '11'

        # we will try to initialize the jvm
        kwargs = {
            'workspace': DEFAULT_WORKSPACE,
            'devroot': DEFAULT_DEVROOT,
            'verbose': False,
            'propfile': DEFAULT_PROPFILE,
            'java_home': os.environ.get('JDK_HOME', None),
            'jvm_properties': {'PyObject.cleanup_on_thread': 'false'},
            'jvm_options': {'-Djava.awt.headless=true',
                            '-DMetricsManager.enabled=true',
                            # '-Xms1g',
                            # '-Xmn512m',
                            '-XX:+UseG1GC',
                            '-XX:MaxGCPauseMillis=100',
                            '-XX:+UseStringDeduplication',
                            '-XX:InitialRAMPercentage=25.0',
                            '-XX:MinRAMPercentage=70.0',
                            '-XX:MaxRAMPercentage=80.0',
                            # '-XshowSettings:vm',
                            # '-verbose:gc', '-XX:+PrintGCDetails',
                            },
            # 'jvm_maxmem': '1g',
            'jvm_classpath': DEFAULT_CLASSPATH,
            'skip_default_classpath': True
        }
        # initialize the jvm
        start_jvm(**kwargs)

        # set up a Deephaven Python session
        py_scope_jpy = jpy.get_type("io.deephaven.engine.util.PythonScopeJpyImpl").ofMainGlobals()
        py_dh_session = jpy.get_type("io.deephaven.engine.util.PythonDeephavenSession")(py_scope_jpy)
        jpy.get_type("io.deephaven.engine.table.lang.QueryScope").setScope(py_dh_session.newQueryScope())

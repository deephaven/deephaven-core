import os
from deephaven2.utils.start_jvm import start_jvm
import jpy


def build_py_session():
    if not jpy.has_jvm():
        DEFAULT_DEVROOT = os.environ.get('DEEPHAVEN_DEVROOT', "/tmp/pyintegration")
        DEFAULT_WORKSPACE = os.environ.get('DEEPHAVEN_WORKSPACE', "/tmp")
        DEFAULT_PROPFILE = os.environ.get('DEEPHAVEN_PROPFILE',  'dh-defaults.prop')
        DEFAULT_CLASSPATH = os.environ.get('DEEPHAVEN_CLASSPATH', "/app/classese/*:/app/libs/*")
        os.environ['JAVA_VERSION'] = '1.8'
        os.environ['JDK_HOME'] = '/usr/lib/jvm/zulu8/jre/'

        # we will try to initialize the jvm
        kwargs = {
            'workspace': DEFAULT_WORKSPACE,
            'devroot': DEFAULT_DEVROOT,
            'verbose': False,
            'propfile': DEFAULT_PROPFILE,
            'java_home': os.environ.get('JDK_HOME', None),
            'jvm_properties': {'PyObject.cleanup_on_thread': 'false'},
            'jvm_options': {'-Djava.awt.headless=true',
                            '-Xms1g',
                            '-Xmn512m',
                            # '-verbose:gc', '-XX:+PrintGCDetails',
                            },
            'jvm_maxmem': '1g',
            'jvm_classpath': DEFAULT_CLASSPATH,
            'skip_default_classpath': True
        }
        # initialize the jvm
        start_jvm(**kwargs)

        # set up a Deephaven Python session
        py_scope_jpy = jpy.get_type("io.deephaven.db.util.PythonScopeJpyImpl").ofMainGlobals()
        py_dh_session = jpy.get_type("io.deephaven.db.util.PythonDeephavenSession")(py_scope_jpy)
        jpy.get_type("io.deephaven.db.tables.select.QueryScope").setScope(py_dh_session.newQueryScope())

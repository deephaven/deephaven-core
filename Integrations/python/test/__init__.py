import os
import jpy

from deephaven_legacy import start_jvm


DEFAULT_DEVROOT = os.environ.get('DEEPHAVEN_DEVROOT', None)

DEFAULT_WORKSPACE = os.environ.get('DEEPHAVEN_WORKSPACE', None)

DEFAULT_PROPFILE = os.environ.get('DEEPHAVEN_PROPFILE', None)

DEFAULT_CLASSPATH = os.environ.get('DEEPHAVEN_CLASSPATH', None)


if __name__ == 'test':
    if not jpy.has_jvm():
        # we will try to initialize the jvm
        kwargs = {
            'workspace': DEFAULT_WORKSPACE,
            'devroot': DEFAULT_DEVROOT,
            'verbose': False,
            'propfile': DEFAULT_PROPFILE,
            'java_home': os.environ.get('JDK_HOME', None),
            'jvm_properties': {},
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


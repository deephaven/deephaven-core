#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import glob
import itertools
import os
import pathlib

from deephaven_internal import jvm

# TODO(deephaven-core#2592): Generalize start_jvm to work with importlib.resources

def _jars_path():
    return pathlib.Path(__file__).parent / 'jars'

def _compiler_directives():
    return _jars_path() / 'dh-compiler-directives.txt'

def _jars():
    return _jars_path().glob('*.jar')

DEFAULT_JVM_PROPERTIES = {
    # Default to no init scripts, the built-in py init script will prevent using python stdin
    'PythonDeephavenSession.initScripts': '',
    # TODO (deephaven-core#XXXX) this doesn't work yet
    # # Disable the browser console by default, this is not yet well supported
    # 'deephaven.console.disable': 'true',
}
DEFAULT_JVM_ARGS = [
    # Uee the G1 GC
    '-XX:+UseG1GC',
    # G1GC: Set a goal for the max duration of a GC pause
    '-XX:MaxGCPauseMillis=100',
    # G1GC: Try to deduplicate strings on the Java heap
    '-XX:+UseStringDeduplication',

    # Disable the JVM's signal handling for interactive python consoles - if python will
    # not be handling signals like ctrl-c (for KeyboardInterrupt), this should be safe to
    # remove for a small performance gain.
    '-Xrs',

    # Disable JIT in certain cases
    '-XX:+UnlockDiagnosticVMOptions',
    f"-XX:CompilerDirectivesFile={_compiler_directives()}",
    # (deephaven-core#2500): Remove DisableIntrinsic for currentThread
    '-XX:DisableIntrinsic=_currentThread',
]

# Provide a util func to start the JVM, will use its own defaults if none are offered
def start_jvm(
        jvm_args = DEFAULT_JVM_ARGS,
        jvm_properties = DEFAULT_JVM_PROPERTIES,
        java_home: str = os.environ.get('JAVA_HOME', None),
        extra_classpath = [],
        propfile: str = None,
        devroot: str = '.',
        workspace: str = '.',
        config = None):
    """ This function uses the default DH property file to embed the Deephaven server and starts a Deephaven Python
    Script session. """

    if propfile is None:
        propfile = 'dh-defaults.prop'

    # Build jvm system properties starting with defaults we accept as args
    system_properties = {
        'devroot': os.path.realpath(devroot),
        'workspace': workspace,
        'Configuration.rootFile': propfile,
    }
    # Append user-created args, allowing them to override these values
    system_properties.update(jvm_properties)

    # Expand the classpath, so a user can resolve wildcards
    expanded_classpath = list(itertools.chain.from_iterable(glob.iglob(e, recursive=True) for e in extra_classpath))

    # The full classpath is the classpath needed for our server + the expanded extra classpath
    jvm_classpath = [str(jar) for jar in _jars()] + expanded_classpath

    # Append args that, if missing, could cause the jvm to be misconfigured for deephaven and its dependencies
    # TODO make these less required (i.e. at your own risk, remove them)
    required_jvm_args = [
        # Allow netty to access java.nio.Buffer fields
        '--add-opens=java.base/java.nio=ALL-UNNAMED',
        # Allow our hotspotImpl project to access internals
        '--add-opens=java.management/sun.management=ALL-UNNAMED',
    ]
    if jvm_args is None:
        jvm_args = required_jvm_args
    else:
        jvm_args.extend(required_jvm_args)

    jvm.init_jvm(
        java_home=java_home,
        # jvm_dll=jvm_dll,
        jvm_classpath=jvm_classpath,
        jvm_properties=system_properties,
        jvm_options=jvm_args,
        # config_file=config_file,
        config=config)

    import jpy
    jpy.VerboseExceptions.enabled = True

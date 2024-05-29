#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import glob
import itertools
import os
import pathlib
from typing import Optional, List, Dict

from deephaven_internal import jvm

# TODO(deephaven-core#2592): Generalize start_jvm to work with importlib.resources

def _jars_path():
    return pathlib.Path(__file__).parent / 'jars'

def _compiler_directives():
    return _jars_path() / 'dh-compiler-directives.txt'

def _default_vmoptions():
    return _jars_path() / 'dh-default.vmoptions'

def _jars():
    return _jars_path().glob('*.jar')

DEFAULT_JVM_PROPERTIES = {
    # Default to no init scripts, the built-in py init script will prevent using python stdin
    'PythonDeephavenSession.initScripts': '',
    # TODO (deephaven-core#XXXX) this doesn't work yet
    # # Disable the browser console by default, this is not yet well supported
    # 'deephaven.console.disable': 'true',
    'LoggerFactory.silenceOnProcessEnvironment': 'true',
    'stdout.toLogBuffer': 'false',
    'stderr.toLogBuffer': 'false',
    'logback.configurationFile': 'logback-minimal.xml',
}
DEFAULT_JVM_ARGS = [
    # Disable JIT in certain cases
    '-XX:+UnlockDiagnosticVMOptions',
    f"-XX:CompilerDirectivesFile={_compiler_directives()}",
    f"-XX:VMOptionsFile={_default_vmoptions()}",
]

def _merge_with_default_jvm_args(jvm_args: Optional[List[str]] = None) -> List[str]:
    """ Merge the default JVM args with the user-provided JVM args."""
    if not jvm_args:
        return DEFAULT_JVM_ARGS

    result_args = DEFAULT_JVM_ARGS.copy()

    # Remove any conflicting options
    for jvm_arg in jvm_args:
        # -XX boolean options
        if jvm_arg.startswith("-XX:+") or jvm_arg.startswith("-XX:-"):
            opt = jvm_arg[5:]
            result_args = [arg for arg in result_args if not arg.endswith(opt)]
            # result_args.append(jvm_arg)
        # -XX numeric/string options
        elif jvm_arg.startswith("-XX:"):
            opt, val, *_ = jvm_arg[4:].split('=', 1)
            result_args = [arg for arg in result_args if arg.startswith("-XX:") and opt != arg[4:].split('=', 1)[0]]
            # result_args.append(jvm_arg)
        # -X options
        elif jvm_arg.startswith("-X"):
            opt, val = jvm_arg[2:].split(':', 1)
            result_args = [arg for arg in result_args if not arg.startswith("-X") or opt != arg[2:].split(':', 1)[0]]
        # JVM system properties
        elif jvm_arg.startswith("-D"):
            opt, val = jvm_arg[2:].split('=', 1)
            result_args = [arg for arg in result_args if not arg.startswith("-D") or opt != arg[2:].split('=', 1)[0]]
        # JPMS - Java Platform Module System options
        elif jvm_arg.startswith("--add-opens=") or jvm_arg.startswith("--add-exports="):
            ...

    # Append the user-provided JVM args
    result_args.extend(jvm_args)
    return result_args

# Provide a util func to start the JVM, will use its own defaults if none are offered
def start_jvm(
        jvm_args: Optional[List[str]] = None,
        jvm_properties: Optional[Dict[str, str]] = None,
        java_home: Optional[str] = None,
        extra_classpath: Optional[List[str]] = None,
        prop_file: str = None,
        config = None,
) -> None:
    """ This function uses the default DH property file to embed the Deephaven server and starts a Deephaven Python
    Script session. """
    jvm_args = _merge_with_default_jvm_args(jvm_args=jvm_args)

    jvm_properties = jvm_properties or DEFAULT_JVM_PROPERTIES
    java_home = java_home or os.environ.get('JAVA_HOME', None)

    system_properties = dict()
    if prop_file:
        # Build jvm system properties starting with defaults we accept as args
        system_properties.update({ 'Configuration.rootFile': prop_file})

    # Append user-created args, allowing them to override these values
    system_properties.update(jvm_properties)

    # Expand the classpath, so a user can resolve wildcards
    if extra_classpath is None:
        expanded_classpath = []
    else:
        expanded_classpath = list(itertools.chain.from_iterable(glob.iglob(e, recursive=True) for e in extra_classpath))

    # The full classpath is the classpath needed for our server + the expanded extra classpath
    jvm_classpath = [str(jar) for jar in _jars()] + expanded_classpath

    # Append args that, if missing, could cause the jvm to be misconfigured for deephaven and its dependencies
    # TODO make these less required (i.e. at your own risk, remove them)
    required_jvm_args = [
        # Allow access to java.nio.Buffer fields
        '--add-opens=java.base/java.nio=ALL-UNNAMED',
        # Allow our hotspot-impl project to access internals
        '--add-exports=java.management/sun.management=ALL-UNNAMED',
        # Allow our clock-impl project to access internals
        '--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED',
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

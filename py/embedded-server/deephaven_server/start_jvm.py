import os
import re
from glob import glob
import jpyutil

DEFAULT_JVM_PROPERTIES = {
    # Allow jpy to clean up its py objects from java on a separate java thread
    'PyObject.cleanup_on_thread': 'true',
    # # Disable any java UI
    # #TODO this is probably no longer required
    # 'java.awt.headless': 'true',
    #TODO probably should be disabled by default, as it is from the JVM?
    'MetricsManager.enabled': 'true',
    # Default to no init scripts, the built-in py init script will prevent using python stdin
    'PythonDeephavenSession.initScripts': '',
}
DEFAULT_JVM_ARGS = [
    # # Ask the JVM to optimize more aggressively
    # '-server',
    # Uee the G1 GC
    '-XX:+UseG1GC',
    # G1GC: Set a goal for the max duration of a GC pause
    '-XX:MaxGCPauseMillis=100',
    # G1GC: Try to deduplicate strings on the Java heap
    '-XX:+UseStringDeduplication',
    # Disable the JVM's signal handling for interactive python consoles
    '-Xrs',
]

# Provide a util func to start the JVM, will use its own defaults if none are offered
def start_jvm(
        jvm_args = DEFAULT_JVM_ARGS,
        jvm_properties = DEFAULT_JVM_PROPERTIES,
        java_home: str = os.environ.get('JAVA_HOME', None),
        classpath = [],
        propfile: str = None,
        devroot: str = '.',
        workspace: str = '.',
        config = None):
    """ This function uses the default DH property file to embed the Deephaven server and starts a Deephaven Python
    Script session. """

    if propfile is None:
        # TODO make this absolute inside the wheel to be unambiguous
        propfile = 'dh-defaults.prop'

    # Build jvm system properties starting with defaults we accept as args
    system_properties = {
        'devroot': os.path.realpath(devroot),
        'workspace': workspace,
        'Configuration.rootFile': propfile,
    }
    # Append user-created args, allowing them to override these values
    system_properties.update(jvm_properties)

    # Build the classpath and expand wildcards, if any
    jvm_classpath = [os.path.join(os.path.dirname(__file__), 'jars', '*.jar')]
    jvm_classpath.extend(classpath)
    jvm_classpath = _expandWildcardsInList(jvm_classpath)

    # Append args that, if missing, could cause the jvm to be misconfigured for deephaven and its dependencies
    # TODO make these less required (i.e. at your own risk, remove them)
    REQUIRED_JVM_ARGS = [
        # Allow netty to access java.nio.Buffer fields
        '--add-opens=java.base/java.nio=ALL-UNNAMED',
        # Allow our hotspotImpl project to access internals
        '--add-opens=java.management/sun.management=ALL-UNNAMED',
        # TODO package, append compiler directives
        # '-XX:+UnlockDiagnosticVMOptions',
        # '-XX:CompilerDirectivesFile=dh-compiler-directives.txt',
    ]
    if (jvm_args is None):
        jvm_args = REQUIRED_JVM_ARGS
    else:
        jvm_args.extend(REQUIRED_JVM_ARGS)


    jpyutil.init_jvm(
        java_home=java_home,
        # jvm_dll=jvm_dll,
        jvm_classpath=jvm_classpath,
        jvm_properties=system_properties,
        jvm_options=jvm_args,
        # config_file=config_file,
        config=config)
    import jpy
    jpy.VerboseExceptions.enabled = True


def _expandLinks(dirname):
    return os.path.realpath(dirname)

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

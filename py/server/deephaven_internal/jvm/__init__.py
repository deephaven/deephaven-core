_is_ready = False


def ready():
    """Marks the JVM as ready. Should be called by Deephaven implementation code. In the case of the Java server process,
    this should be called right after Python has been initialized. In the case of the embedded Python server process,
    this should be called right after the JVM has been initialized."""
    global _is_ready
    _is_ready = True


def check_ready():
    """Checks if the JVM is ready (ie, if ready() has been called). Should be called by Deephaven implementation code.
    Raises a RuntimeError if the JVM is not ready.

    Raises:
        RuntimeError
    """
    # Note: we might be tempted to store the source of truth for this in Java, but we aren't able to do that.
    # `import jpy` has potential side effects, and may improperly start the JVM.
    global _is_ready
    if not _is_ready:
        raise RuntimeError("The Deephaven Server has not been initialized. "
                        "Please ensure that deephaven_server.Server has been constructed before importing deephaven.")


def check_py_env():
    """Checks if the current Python environment is in good order and if not, raises a RuntimeError.

    Raises:
        RuntimeError
    """
    import importlib.metadata
    try:
        importlib.metadata.version("deephaven")
    except:
        pass
    else:
        # DH Enterprise deephaven package is installed by mistake
        raise RuntimeError("The Deephaven Enterprise Python Package (name 'deephaven' on pypi) is installed in "
                           "the current Python environment. It conflicts with the Deephaven Community Python "
                           "Package (name 'deephaven-core' on pypi). Please uninstall the 'deephaven' package and "
                           "reinstall the 'deephaven-core' package.")


def preload_jvm_dll(*args, **kwargs):
    """A wrapper around jpyutil.preload_jvm_dll(...)."""
    import jpyutil
    result = jpyutil.preload_jvm_dll(*args, **kwargs)
    return result


def init_jvm(*args, **kwargs):
    """A wrapper around jpyutil.init_jvm(...). Should be called by Deephaven implementation code.
    Calls ready() after jpyutil.init_jvm(...).

    Raises:
        ImportError
    """
    # Note: we might be able to use our own logic instead of jpyutil here in the future
    import jpyutil
    try:
        result = jpyutil.init_jvm(*args, **kwargs)
    except ImportError as e:
        raise ImportError(
            "Unable to initialize JVM, try setting the environment variable JAVA_HOME (JDK 11+ required)") from e
    ready()
    return result

def init_py():
    """Finishes starting Python to be usable from inside of a Java process. Not intended to be called in cases
    where the process was started as Python, and Java was started from inside Python.
    """

    import jpy
    import os
    import sys
    from deephaven_internal.stream import TeeStream

    # Set stdin to /dev/null to prevent functions (like help()) that attempt to read from stdin from hanging python
    # execution from within Java.
    os.dup2(os.open("/dev/null", os.O_RDONLY), 0)

    jpy.VerboseExceptions.enabled = True
    # If you want jpy to tell you about all that it is doing, change this
    # jpy.diag.flags = jpy.diag.F_ALL

    j_sys = jpy.get_type('java.lang.System')
    sys.stdout = TeeStream.redirect(sys.stdout, j_sys.out)
    sys.stderr = TeeStream.redirect(sys.stderr, j_sys.err)

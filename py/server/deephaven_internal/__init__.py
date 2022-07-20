_jvm_is_ready = False

def jvm_ready():
    """Marks the JVM as ready. Should be called by Deephaven implementation code. In the case of the Java server process,
    this should be called right after Python has been initialized. In the case of the embedded Python server process,
    this should be called right after the JVM has been initialized."""
    global _jvm_is_ready
    _jvm_is_ready = True

def check_jvm():
    """Checks if the JVM is ready. Should be called by Deephaven implementation code. Raises an exception if the JVM is
    not ready."""
    # Note: we might be tempted to store the source of truth for this in Java, but we aren't able to do that.
    # `import jpy` has potential side effects, and may improperly start the JVM.
    global _jvm_is_ready
    if not _jvm_is_ready:
        raise Exception("The Deephaven Server has not been initialized. "
                        "Please ensure that deephaven_server.Server has been constructed before importing deephaven.")

def preload_jvm_dll(*args, **kwargs):
    """A wrapper around jpyutil.preload_jvm_dll(...)."""
    import jpyutil
    result = jpyutil.preload_jvm_dll(*args, **kwargs)
    return result

def init_jvm(*args, **kwargs):
    """A wrapper around jpyutil.init_jvm(...). Should be called by Deephaven implementation code.
    Calls jvm_ready() after jpyutil.init_jvm(...)."""
    # Note: we might be able to use our own logic instead of jpyutil here in the future
    import jpyutil
    try:
        result = jpyutil.init_jvm(*args, **kwargs)
    except ImportError as e:
        raise ImportError("Unable to initialize JVM, try setting the environment variable JAVA_HOME (JDK 11+ required)") from e
    jvm_ready()
    return result

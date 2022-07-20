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
                        "Please ensure that deephaven_server.Server has been constructed, "
                        "or that deephaven_server.start_jvm() has been invoked, "
                        "before importing deephaven.")

def preload_jvm_dll(*args, **kwargs):
    import jpyutil
    result = jpyutil.preload_jvm_dll(*args, **kwargs)
    return result

def init_jvm(*args, **kwargs):
    import jpyutil
    result = jpyutil.init_jvm(*args, **kwargs)
    jvm_ready()
    return result

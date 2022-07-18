_server_is_ready = False

def server_ready():
    """Marks the server as ready"""
    global _server_is_ready
    _server_is_ready = True

def check_server():
    """Checks if the server is ready. Raises an exception if the server is not ready."""
    global _server_is_ready
    if not _server_is_ready:
        raise Exception("The Deephaven Server has not been initialized. "
                        "Please ensure that deephaven_server.Server has been constructed, "
                        "or that deephaven_server.start_jvm() has been invoked, "
                        "before importing deephaven.")

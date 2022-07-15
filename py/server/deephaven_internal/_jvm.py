from . import _server

if not _server.is_ready:
    raise Exception("The Deephaven Server has not been initialized. "
                    "Please ensure that deephaven_server.Server has been constructed, "
                    "or that deephaven_server.start_jvm() has been invoked, "
                    "before importing deephaven.")

import jpy

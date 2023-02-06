#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from typing import Dict, List, Optional
import sys

from .start_jvm import start_jvm

# This is explicitly not a JObjectWrapper, as that would require importing deephaven and jpy
# before the JVM was running.
class Server:
    """
    Represents a Deephaven server that can be created from Python.
    """

    instance = None

    @property
    def j_object(self):
        return self.j_server

    @property
    def port(self):
        return self.j_server.getPort()

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None, jvm_args: Optional[List[str]] = None, dh_args: Dict[str, str] = {}):
        """
        Creates a Deephaven embedded server. Only one instance can be created at this time.
        """
        # TODO deephaven-core#2453 consider providing @dataclass for arguments

        # If the server was already created, emit an error to warn away from trying again
        if Server.instance is not None:
            from deephaven import DHError
            raise DHError('Cannot create more than one instance of the server')
        # given the jvm args, ensure that the jvm has started
        start_jvm(jvm_args=jvm_args)

        # it is now safe to import jpy
        import jpy

        # Create a python-wrapped java server that we can reference to talk to the platform
        self.j_server = jpy.get_type('io.deephaven.python.server.EmbeddedServer')(host, port, dh_args)

        # Obtain references to the deephaven logbuffer and redirect stdout/stderr to it. Note that we should not import
        # this until after jpy has started.
        from deephaven_internal.stream import TeeStream
        sys.stdout = TeeStream.split(sys.stdout, self.j_server.getStdout())
        sys.stderr = TeeStream.split(sys.stderr, self.j_server.getStderr())

        # Keep a reference to the server so we know it is running
        Server.instance = self

    def start(self):
        """
        Starts the server. Presently once the server is started, it cannot be stopped until the
        python process halts.
        """
        self.j_server.start()

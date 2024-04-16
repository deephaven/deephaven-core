#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

from typing import Dict, List, Optional
import sys

from .start_jvm import start_jvm

# These classes are explicitly not JObjectWrapper, as that would require importing deephaven and jpy
# before the JVM was running.


class ServerConfig:
    """
    Represents the configuration of a Deephaven server.
    """

    def __init__(self, j_server_config):
        self.j_server_config = j_server_config

    @property
    def j_object(self):
        return self.j_server_config

    @property
    def target_url_or_default(self) -> str:
        """
        Returns the target URL to bring up the Web UI.

        Returns:
            The target URL to bring up the Web UI.
        """
        return self.j_server_config.targetUrlOrDefault()


class AuthenticationHandler:
    """
    Represents an authentication handler for a Deephaven server.
    """
    def __init__(self, j_authentication_handler):
        self.j_authentication_handler = j_authentication_handler

    @property
    def j_object(self):
        return self.j_authentication_handler

    @property
    def auth_type(self) -> str:
        """
        Get the authentication type for this handler.

        Returns:
            The authentication type for this handler.
        """
        return self.j_authentication_handler.getAuthType()

    def urls(self, target_url: str) -> List[str]:
        """
        Get the URLs for this authentication handler.

        Args:
            target_url: The target URL where the Web UI is hosted.

        Returns:
            The URLs provided by this authentication handler to open the Web UI with authentication.
            For example, with pre-shared key authentication, it could add a query param with the pre-shared key.

        """
        return list(self.j_authentication_handler.urls(target_url).toArray())


class Server:
    """
    Represents a Deephaven server that can be created from Python.
    """

    instance = None

    @property
    def j_object(self):
        return self.j_server

    @property
    def port(self) -> int:
        """
        Get the port the server is running on.

        Returns:
            The port the server is running on.
        """
        return self.j_server.getPort()

    @property
    def server_config(self) -> ServerConfig:
        """
        Get the configuration of the server.

        Returns:
            The configuration of the server.
        """
        return ServerConfig(self.j_server.serverConfig())

    @property
    def authentication_handlers(self) -> List[AuthenticationHandler]:
        """
        Get the authentication handlers for the server.

        Returns:
            The authentication handlers for the server.
        """
        return [
            AuthenticationHandler(j_auth_handler)
            for j_auth_handler in self.j_server.authenticationHandlers().toArray()
        ]

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        jvm_args: Optional[List[str]] = None,
        extra_classpath: Optional[List[str]] = None,
    ):
        """
        Creates a Deephaven embedded server. Only one instance can be created at this time.
        """
        # TODO deephaven-core#2453 consider providing @dataclass for arguments

        # If the server was already created, emit an error to warn away from trying again
        if Server.instance is not None:
            from deephaven import DHError

            raise DHError("Cannot create more than one instance of the server")
        if extra_classpath is None:
            extra_classpath = []

        # given the jvm args, ensure that the jvm has started
        start_jvm(jvm_args=jvm_args, extra_classpath=extra_classpath)

        # it is now safe to import jpy
        import jpy

        # Create a python-wrapped java server that we can reference to talk to the platform
        self.j_server = jpy.get_type("io.deephaven.python.server.EmbeddedServer")(host, port)

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

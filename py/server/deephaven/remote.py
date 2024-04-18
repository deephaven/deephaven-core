#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations
import threading
from typing import Dict

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.table import Table

_JURI = jpy.get_type("java.net.URI")
_JClientConfig = jpy.get_type("io.deephaven.client.impl.ClientConfig")
_JSessionImplConfig = jpy.get_type("io.deephaven.client.impl.SessionImplConfig")
_JDeephavenTarget = jpy.get_type("io.deephaven.uri.DeephavenTarget")
_JChannelHelper = jpy.get_type("io.deephaven.client.impl.ChannelHelper")
_JDeephavenChannelImpl = jpy.get_type("io.deephaven.proto.DeephavenChannelImpl")
_JSessionImpl = jpy.get_type("io.deephaven.client.impl.SessionImpl")
_JExecutors = jpy.get_type("java.util.concurrent.Executors")
_JBarrageSession = jpy.get_type("io.deephaven.client.impl.BarrageSession")
_JRootAllocator = jpy.get_type("org.apache.arrow.memory.RootAllocator")
_JSharedId = jpy.get_type("io.deephaven.client.impl.SharedId")
_JBarrageTableResolver = jpy.get_type("io.deephaven.server.uri.BarrageTableResolver")

_session_cache: Dict[str, RemoteSession] = {}  # use WeakValueDictionary to avoid memory leak?
_remote_session_lock = threading.Lock()


def remote_session(host: str,
                   port: int = 10000,
                   auth_type: str = "Anonymous",
                   auth_token: str = "",
                   # never_timeout: bool = True,
                   # session_type: str = 'python',
                   # use_tls: bool = False,
                   # tls_root_certs: bytes = None,
                   # client_cert_chain: bytes = None,
                   ) -> RemoteSession:
    """Returns a Deephaven gRPC session to a remote server if a cached session is available; otherwise, creates a new
    session.

    Args:
        host (str): the host name or IP address of the Deephaven server.
        port (int): the port number that the remote Deephaven server is listening on, default is 10000.
        auth_type (str): the authentication type string, can be "Anonymous', 'Basic", or any custom-built
            authenticator in the server, such as "io.deephaven.authentication.psk.PskAuthenticationHandler",
            default is 'Anonymous'.
        auth_token (str): the authentication token string. When auth_type is 'Basic', it must be
            "user:password"; when auth_type is "Anonymous', it will be ignored; when auth_type is a custom-built
            authenticator, it must conform to the specific requirement of the authenticator
    Returns:
        a Deephaven gRPC session

    Raises:
        DHError
    """
    with _remote_session_lock:
        uri = f"dh+plain://{host}:{port}"
        session = _session_cache.get(uri)
        if session:
            if session.is_alive:  # doesn't guarantee the session is still alive, just that it hasn't been explicitly
                # closed
                return session
            else:
                del _session_cache[uri]
        session = RemoteSession(host, port, auth_type, auth_token)
        _session_cache[uri] = session
        return session


class RemoteSession (JObjectWrapper):
    """ A Deephaven gRPC session to a remote server."""
    # def __init__(self, j_barrage_session):
    #     self.j_barrage_session = j_barrage_session
    #
    # def subscribe(self, ticket: bytes):
    #     j_table_handle = self._j_session.of(_JSharedId(ticket).ticketId().table())
    #     j_barrage_subscription = self._j_barrage_session.subscribe(j_table_handle,
    #                                                                _JBarrageTableResolver.SUB_OPTIONS)
    #     j_table = j_barrage_subscription.entireTable().get()
    #     return Table(j_table)
    #
    # def snapshot(self):
    #     return self.j_barrage_session.snapshot()
    def __init__(self, host: str,
                 port: int = 10000,
                 auth_type: str = "Anonymous",
                 auth_token: str = "",
                 # never_timeout: bool = True,
                 # session_type: str = 'python',
                 # use_tls: bool = False,
                 # tls_root_certs: bytes = None,
                 # client_cert_chain: bytes = None,
                 ):
        """Creates a Deephaven gRPC session to a remote server.

        Args:
            host (str): the host name or IP address of the Deephaven server.
            port (int): the port number that the remote Deephaven server is listening on, default is 10000.
            auth_type (str): the authentication type string, can be "Anonymous', 'Basic", or any custom-built
                authenticator in the server, such as "io.deephaven.authentication.psk.PskAuthenticationHandler",
                default is 'Anonymous'.
            auth_token (str): the authentication token string. When auth_type is 'Basic', it must be
                "user:password"; when auth_type is "Anonymous', it will be ignored; when auth_type is a custom-built
                authenticator, it must conform to the specific requirement of the authenticator
        """
        self.host = host
        self.port = port
        self._auth_type = auth_type
        self._auth_token = auth_token
        self.grpc_channel = None
        self._r_lock = threading.RLock()
        self._is_alive = False
        self._uri = f"dh+plain://{host}:{port}"
        self._connect()

    @property
    def is_alive(self) -> bool:
        """if the session is alive."""
        return self._is_alive

    def _connect(self) -> None:
        try:
            _j_host_config = (_JClientConfig.builder()
                              .target(_JDeephavenTarget.of(_JURI(self._uri)))
                              .build())
            self._j_channel = _JChannelHelper.channel(_j_host_config)
            _j_dh_channel = _JDeephavenChannelImpl(self._j_channel)

            _j_session_config = (_JSessionImplConfig.builder()
                                 .executor(_JExecutors.newScheduledThreadPool(4))
                                 .authenticationTypeAndValue(f"{self._auth_type} {self._auth_token}")
                                 .channel(_j_dh_channel)
                                 .build())
            self._j_session = _JSessionImpl.create(_j_session_config)
            self._j_barrage_session = _JBarrageSession.create(self._j_session, _JRootAllocator(), self._j_channel)
            self._is_alive = True
        except Exception as e:
            raise DHError("failed to create a session to a remote Deephaven server.") from e

    def close(self):
        """Closes the gRPC connection."""
        if not self._is_alive:
            return

        with self._r_lock:
            if not self._is_alive:
                return
            try:
                self._j_session.close()
            except Exception as e:
                raise DHError("failed to close the session.") from e
            finally:
                self._is_alive = False

    def fetch(self, shared_ticket: bytes) -> Table:
        """Fetches data from the Deephaven server."""
        if not self._is_alive:
            raise DHError("the session is not alive.")
        try:
            j_table_handle = self._j_session.of(_JSharedId(shared_ticket).ticketId().table())
            j_barrage_subscription = self._j_barrage_session.subscribe(j_table_handle,
                                                                       _JBarrageTableResolver.SUB_OPTIONS)
            j_table = j_barrage_subscription.entireTable().get()
            return Table(j_table)
        except Exception as e:
            raise DHError("failed to fetch data from the server.") from e

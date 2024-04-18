#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations
import threading
from typing import Dict

import jpy

from deephaven import DHError, uri
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

_session_cache: Dict[str, BarrageSession] = {}  # use WeakValueDictionary to avoid memory leak?
_remote_session_lock = threading.Lock()


def barrage_session(host: str,
                    port: int = 10000,
                    auth_type: str = "Anonymous",
                    auth_token: str = "",
                    use_tls: bool = False,
                    tls_root_certs: bytes = None,
                    client_cert_chain: bytes = None,
                    client_private_key: bytes = None,
                    ) -> BarrageSession:
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
        use_tls (bool): if True, use a TLS connection.  Defaults to False
        tls_root_certs (bytes): PEM encoded root certificates to use for TLS connection, or None to use system defaults.
             If not None implies use a TLS connection and the use_tls argument should have been passed
             as True. Defaults to None
        client_cert_chain (bytes): PEM encoded client certificate if using mutual TLS.  Defaults to None,
             which implies not using mutual TLS.
        client_private_key (bytes): PEM encoded client private key for client_cert_chain if using mutual TLS.
             Defaults to None, which implies not using mutual TLS.

    Returns:
        a Deephaven gRPC session

    Raises:
        DHError
    """
    try:
        j_barrage_session_factory_client = uri.resolve(
            "dh:///app/io.deephaven.server.barrage.BarrageSessionFactoryClient/field/instance")

        target_uri = f"{host}:{port}"
        if use_tls:
            target_uri = f"dh://{target_uri}"
        else:
            target_uri = f"dh+plain://{target_uri}"

        _j_client_config = (_JClientConfig.builder()
                          .target(_JDeephavenTarget.of(_JURI(target_uri)))
                          .build())
        auth = f"{auth_type} {auth_token}"
        _j_barrage_session_factory = j_barrage_session_factory_client.factory(_j_client_config, auth)
        return BarrageSession(_j_barrage_session_factory.newBarrageSession())
    except Exception as e:
        raise DHError("failed to get a barrage session to the target remote Deephaven server.") from e


class BarrageSession (JObjectWrapper):
    """ A Deephaven gRPC session to a remote server."""
    j_object_type = _JBarrageSession

    @property
    def j_object(self) -> jpy.JType:
        return self.j_barrage_session

    def __init__(self, j_barrage_session):
        self.j_barrage_session = j_barrage_session
        self.j_session = j_barrage_session.session()

    def subscribe(self, ticket: bytes) -> Table:
        """ TODO

        Args:
            ticket:

        Returns:

        """
        j_table_handle = self.j_session.of(_JSharedId(ticket).ticketId().table())
        j_barrage_subscription = self.j_barrage_session.subscribe(j_table_handle,
                                                                   _JBarrageTableResolver.SUB_OPTIONS)
        return Table(j_barrage_subscription.entireTable().get())

    def snapshot(self, ticket: bytes) -> Table:
        """ TODO

        Args:
            ticket:

        Returns:

        """
        j_table_handle = self.j_session.of(_JSharedId(ticket).ticketId().table())
        j_barrage_snapshot = self.j_barrage_session.snapshot(j_table_handle, _JBarrageTableResolver.SNAP_OPTIONS)
        return Table(j_barrage_snapshot.entireTable().get())



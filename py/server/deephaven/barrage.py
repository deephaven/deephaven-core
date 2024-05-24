#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
""" This module defines the BarrageSession wrapper class and provides a factory function to create an instance of it
 for accessing resources on remote Deephaven servers."""

from __future__ import annotations

import jpy

from deephaven import DHError
from deephaven.table import Table

_JURI = jpy.get_type("java.net.URI")
_JTimeUnit = jpy.get_type("java.util.concurrent.TimeUnit")
_JClientConfig = jpy.get_type("io.deephaven.client.impl.ClientConfig")
_JSSLConfig = jpy.get_type("io.deephaven.ssl.config.SSLConfig")
_JSessionImplConfig = jpy.get_type("io.deephaven.client.impl.SessionImplConfig")
_JSessionConfig = jpy.get_type("io.deephaven.client.impl.SessionConfig")
_JTrustCustom = jpy.get_type("io.deephaven.ssl.config.TrustCustom")
_JDeephavenTarget = jpy.get_type("io.deephaven.uri.DeephavenTarget")
_JBarrageSession = jpy.get_type("io.deephaven.client.impl.BarrageSession")
_JTableSpec = jpy.get_type("io.deephaven.qst.table.TableSpec")
_JBarrageTableResolver = jpy.get_type("io.deephaven.server.uri.BarrageTableResolver")
_JChannelHelper = jpy.get_type("io.deephaven.client.impl.ChannelHelper")
_JDeephavenChannelImpl = jpy.get_type("io.deephaven.proto.DeephavenChannelImpl")
_JSessionImpl = jpy.get_type("io.deephaven.client.impl.SessionImpl")
_JExecutors = jpy.get_type("java.util.concurrent.Executors")
_JRootAllocator = jpy.get_type("org.apache.arrow.memory.RootAllocator")
_JDeephavenApiServer = jpy.get_type("io.deephaven.server.runner.DeephavenApiServer")


class BarrageSession():
    """ A Deephaven Barrage session to a remote server."""

    def __init__(self, j_barrage_session: jpy.JType, j_managed_channel: jpy.JType = None):
        """ Initializes a Barrage session.

        When BarrageSession is created via the barrage_session() factory function, j_managed_channel is always provided,
        and when BarrageSession.close() is called, it will shut down the channel as well as close the j_barrage_session.

        When BarrageSession is initialized directly and j_managed_channel is None, when BarrageSession.close() is called,
        it will only close the j_barrage_session, it is the responsibility of the user to shut down the associated
        channel if needed.
        """
        self.j_barrage_session = j_barrage_session
        self.j_session = j_barrage_session.session()
        self.j_managed_channel = j_managed_channel

    def __enter__(self) -> BarrageSession:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """ Closes the barrage session.

        If the BarrageSession is initialized with a managed channel, the channel will be shut down as well.
        """
        try:
            self.j_barrage_session.close()
            if self.j_managed_channel:
                self.j_managed_channel.shutdownNow()
                self.j_managed_channel.awaitTermination(10, _JTimeUnit.SECONDS)
        except Exception as e:
            raise DHError(e, "failed to close the barrage session.") from e

    def subscribe(self, ticket: bytes) -> Table:
        """ Subscribes to a published remote table with the given ticket.

        Note, if the remote table is closed or its owner session is closed, the ticket becomes invalid. If the same
        ticket is subscribed to multiple times, multiple subscriptions will be created.

        Args:
            ticket (bytes): the bytes of the ticket

        Returns:
            a Table

        Raises:
            DHError
        """
        try:
            j_barrage_subscription = self.j_barrage_session.subscribe(_JTableSpec.ticket(ticket),
                                                                      _JBarrageTableResolver.SUB_OPTIONS)
            return Table(j_barrage_subscription.entireTable().get())
        except Exception as e:
            raise DHError(e, "failed to subscribe to the remote table with the provided ticket.") from e

    def snapshot(self, ticket: bytes) -> Table:
        """ Returns a snapshot of a published remote table with the given ticket.

        Note, if the remote table is closed or its owner session is closed, the ticket becomes invalid. If the same
        ticket is snapshot multiple times, multiple snapshots will be created.

        Args:
            ticket (bytes): the bytes of the ticket

        Returns:
            a Table

        Raises:
            DHError
        """
        try:
            j_barrage_snapshot = self.j_barrage_session.snapshot(_JTableSpec.ticket(ticket), _JBarrageTableResolver.SNAP_OPTIONS)
            return Table(j_barrage_snapshot.entireTable().get())
        except Exception as e:
            raise DHError(e, "failed to take a snapshot of the remote table with the provided ticket.") from e


def barrage_session(host: str,
                    port: int = 10000,
                    auth_type: str = "Anonymous",
                    auth_token: str = "",
                    use_tls: bool = False,
                    tls_root_certs: bytes = None,
                    ) -> BarrageSession:
    """Returns a Deephaven gRPC session to a remote server if a cached session is available; otherwise, creates a new
    session.

    Note: client authentication is not supported yet.

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

    Returns:
        a Deephaven Barrage session

    Raises:
        DHError
    """
    try:
        if tls_root_certs and not use_tls:
            raise DHError(message="tls_root_certs is provided but use_tls is False")

        target_uri = f"{host}:{port}"
        if use_tls:
            target_uri = f"dh://{target_uri}"
        else:
            target_uri = f"dh+plain://{target_uri}"

        j_client_config = _build_client_config(target_uri, tls_root_certs)
        auth = f"{auth_type} {auth_token}"

        try:
            return _get_barrage_session_via_api_server(j_client_config, auth)
        except:
            # fall back to the direct way when we don't have a fully initialized server, used for testing only
            # TODO: remove when we are done with restructuring the integrations tests wiring
            #  https://github.com/deephaven/deephaven-core/issues/5401
            return _get_barrage_session_direct(j_client_config, auth)
    except Exception as e:
        raise DHError(e, "failed to get a barrage session to the target remote Deephaven server.") from e


def _get_barrage_session_via_api_server(client_config: jpy.JType, auth: str) -> BarrageSession:
    j_barrage_session_factory_creator = _JDeephavenApiServer.getInstance().sessionFactoryCreator()
    j_barrage_session_factory = j_barrage_session_factory_creator.barrageFactory(client_config)
    j_managed_channel = j_barrage_session_factory.managedChannel()
    if auth:
        j_session_config = (_JSessionConfig.builder()
                            .authenticationTypeAndValue(auth)
                            .build())
        j_barrage_session = j_barrage_session_factory.newBarrageSession(j_session_config)
    else:
        j_barrage_session = j_barrage_session_factory.newBarrageSession()
    return BarrageSession(j_barrage_session, j_managed_channel)

def _get_barrage_session_direct(client_config: jpy.JType, auth: str) -> BarrageSession:
    """Note, this is used for testing only. This way of constructing a Barrage session is less efficient because it does
    not share any of the state or configuration that the server provides; namely, when you are doing it with the server
    context it provides a singleton executor, allocator, outbound SSL configuration, and the ability for the server to
    hook in additional channel building options.

    TODO: remove when we are done with restructuring the integrations tests wiring
    https://github.com/deephaven/deephaven-core/issues/5401.
    """
    j_channel = _JChannelHelper.channel(client_config)
    j_dh_channel = _JDeephavenChannelImpl(j_channel)

    j_session_config = (_JSessionImplConfig.builder()
                        .executor(_JExecutors.newScheduledThreadPool(4))
                        .authenticationTypeAndValue(auth)
                        .channel(j_dh_channel)
                        .build())
    try:
        j_session = _JSessionImpl.create(j_session_config)
    except Exception as e:
        # if the connection to the host can't be established, we should clean up the resources
        j_session_config.executor().shutdownNow()
        j_channel.shutdownNow()
        j_channel.awaitTermination(10, _JTimeUnit.SECONDS)
        raise

    j_barrage_session = _JBarrageSession.create(j_session, _JRootAllocator(), j_channel)
    return BarrageSession(j_barrage_session, j_channel)


def _build_client_config(target_uri: str, tls_root_certs: bytes) -> jpy.JType:
    j_client_config_builder = _JClientConfig.builder()
    j_client_config_builder.target(_JDeephavenTarget.of(_JURI(target_uri)))
    if tls_root_certs:
        j_ssl_config = _JSSLConfig.builder().trust(
            _JTrustCustom.ofX509(tls_root_certs, 0, len(tls_root_certs))).build()
        j_client_config_builder.ssl(j_ssl_config)
    j_client_config = j_client_config_builder.build()
    return j_client_config

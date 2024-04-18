#
#   Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import threading

import jpy

from deephaven import DHError
from deephaven.table import Table

_JURI = jpy.get_type("java.net.URI")
_JClientConfig = jpy.get_type("io.deephaven.client.impl.ClientConfig")
_JSessionImplConfig = jpy.get_type("io.deephaven.client.impl.SessionImplConfig")
_JDeephavenTarget = jpy.get_type("io.deephaven.uri.DeephavenTarget")
_JChannelHelper = jpy.get_type("io.deephaven.client.impl.ChannelHelper")
_JDeephavenChannelImpl = jpy.get_type("io.deephaven.proto.DeephavenChannelImpl")
_JSessionImpl = jpy.get_type("io.deephaven.client.impl.SessionImpl")
_JExecutors = jpy.get_type("java.util.concurrent.Executors")
# _JBarrageSession = jpy.get_type("io.deephaven.client.impl.BarrageSession")
# _JFlightSession = jpy.get_type("io.deephaven.client.impl.FlightSession")
# _JRootAllocator = jpy.get_type("org.apache.arrow.memory.RootAllocator")


class Session:
    """
    A Deephaven gRPC session.
    """
    def __init__(self, host: str = None,
                 port: int = None,
                 auth_type: str = "Anonymous",
                 auth_token: str = ""):
        self.host = host
        self.port = port
        self._auth_type = auth_type
        self._auth_token = auth_token
        self.grpc_channel = None
        self._r_lock = threading.RLock()
        self._is_alive = False
        self._connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _connect(self) -> None:
        target = ":".join([self.host, str(self.port)])
        try:
            _j_host_config = (_JClientConfig.builder()
                              .target(_JDeephavenTarget.of(_JURI("dh+plain://" + target)))
                              .build())
            _j_channel = _JChannelHelper.channel(_j_host_config)
            _j_dh_channel = _JDeephavenChannelImpl(_j_channel)

            _j_session_config = (_JSessionImplConfig.builder()
                                 .executor(_JExecutors.newScheduledThreadPool(4))
                                 .authenticationTypeAndValue(f"{self._auth_type} {self._auth_token}")
                                 .channel(_j_dh_channel)
                                 .build())
            self._j_session = _JSessionImpl.create(_j_session_config)
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

    def fetch(self, shared_ticket) -> Table:
        """Fetches data from the Deephaven server."""
        if not self._is_alive:
            raise DHError("the session is not alive.")
        try:
            return Table(self._j_session.of(shared_ticket).table())
        except Exception as e:
            raise DHError("failed to fetch data from the server.") from e

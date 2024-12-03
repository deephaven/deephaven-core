#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import grpc

from pydeephaven.dherror import DHError
from deephaven_core.proto import session_pb2_grpc, session_pb2, ticket_pb2
from pydeephaven.ticket import Ticket, ExportTicket


class SessionService:
    def __init__(self, session):
        self.session = session
        self._grpc_session_stub = None

    def connect(self) -> grpc.Channel:
        """Connects to the server and returns a gRPC channel upon success."""
        target = ":".join([self.session.host, str(self.session.port)])
        if self.session._use_tls:
            credentials = grpc.ssl_channel_credentials(
                root_certificates=self.session._tls_root_certs,
                private_key=self.session._client_private_key,
                certificate_chain=self.session._client_cert_chain)
            grpc_channel = grpc.secure_channel(target, credentials, self.session._client_opts)
        else:
            grpc_channel = grpc.insecure_channel(target, self.session._client_opts)
        self._grpc_session_stub = session_pb2_grpc.SessionServiceStub(grpc_channel)
        return grpc_channel

    def close(self):
        """Closes the gRPC connection."""
        try:
            self.session.wrap_rpc(
                self._grpc_session_stub.CloseSession,
                session_pb2.HandshakeRequest(
                    auth_protocol=0,
                    payload=self.session._auth_header_value))
        except Exception as e:
            raise DHError("failed to close the session.") from e

    def release(self, ticket: ExportTicket) -> None:
        """Releases an export ticket."""
        try:
            self.session.wrap_rpc(
                self._grpc_session_stub.Release,
                session_pb2.ReleaseRequest(id=ticket.pb_ticket))
        except Exception as e:
            raise DHError("failed to release a ticket.") from e

    def publish(self, source_ticket: Ticket, result_ticket: Ticket) -> None:
        """Makes a copy from the source ticket and publishes it to the result ticket.

        Args:
            source_ticket (Ticket): The source ticket to publish from.
            result_ticket (Ticket): The result ticket to publish to.

        Raises:
            DHError: If the operation fails.
        """
        try:
            self.session.wrap_rpc(
                self._grpc_session_stub.PublishFromTicket,
                session_pb2.PublishRequest(
                    source_id=source_ticket.pb_ticket,
                    result_id=result_ticket.pb_ticket))
        except Exception as e:
            raise DHError("failed to publish a ticket.") from e

    def fetch(self, ticket: Ticket) -> ExportTicket:
        """Fetches a typed ticket from a ticket.

        Args:
            ticket: The ticket to fetch from.

        Returns:
            The typed ticket.
        """
        try:
            result_id = self.session.make_export_ticket()
            self.session.wrap_rpc(
                self._grpc_session_stub.ExportFromTicket,
                session_pb2.ExportRequest(source_id=ticket.pb_ticket, result_id=result_id.pb_ticket))
            return result_id
        except Exception as e:
            raise DHError("failed to fetch a ticket.") from e
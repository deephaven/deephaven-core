#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import grpc

from pydeephaven.dherror import DHError
from pydeephaven.proto import session_pb2_grpc, session_pb2


class SessionService:
    def __init__(self, session):
        self.session = session
        self._grpc_session_stub = None

    def connect(self):
        grpc_channel = grpc.insecure_channel(":".join([self.session.host, str(self.session.port)]))
        self._grpc_session_stub = session_pb2_grpc.SessionServiceStub(grpc_channel)

        try:
            response = self._grpc_session_stub.NewSession(
                session_pb2.HandshakeRequest(auth_protocol=1, payload=b'hello pydeephaven'))
            return grpc_channel, response.session_token, response.token_expiration_delay_millis
        except Exception as e:
            grpc_channel.close()
            raise DHError("failed to connect to the server.") from e

    def refresh_token(self):
        try:
            response = self._grpc_session_stub.RefreshSessionToken(
                session_pb2.HandshakeRequest(auth_protocol=0, payload=self.session.session_token),
                metadata=self.session.grpc_metadata)
            return response.session_token, response.token_expiration_delay_millis
        except Exception as e:
            raise DHError("failed to refresh session token.") from e

    def close(self):
        try:
            self._grpc_session_stub.CloseSession(
                session_pb2.HandshakeRequest(auth_protocol=0, payload=self.session.session_token),
                metadata=self.session.grpc_metadata)
        except Exception as e:
            raise DHError("failed to close the session.") from e

    def release(self, ticket):
        try:
            self._grpc_session_stub.Release(session_pb2.ReleaseRequest(id=ticket), metadata=self.session.grpc_metadata)
        except Exception as e:
            raise DHError("failed to release a ticket.") from e

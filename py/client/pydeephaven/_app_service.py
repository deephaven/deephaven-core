#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from typing import Any

from pydeephaven.dherror import DHError
from pydeephaven.proto import application_pb2_grpc, application_pb2


class AppService:
    def __init__(self, session):
        self.session = session
        self._grpc_app_stub = application_pb2_grpc.ApplicationServiceStub(session.grpc_channel)

    def list_fields(self) -> Any:
        """Fetches the current application fields."""
        try:
            fields = self._grpc_app_stub.ListFields(
                application_pb2.ListFieldsRequest(),
                metadata=self.session.grpc_metadata
            )
            return fields
        except Exception as e:
            raise DHError("failed to list fields.") from e

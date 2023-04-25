#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from typing import Any, Dict

from pydeephaven.dherror import DHError
from pydeephaven.proto import config_pb2, config_pb2_grpc


class ConfigService:
    def __init__(self, session):
        self.session = session
        self._grpc_app_stub = config_pb2_grpc.ConfigServiceStub(session.grpc_channel)

    def get_configuration_constants(self) -> Dict[str, Any]:
        """Fetches the server configuration as a dict."""
        try:
            response = self._grpc_app_stub.GetConfigurationConstants(config_pb2.ConfigurationConstantsRequest(),
                                                                     metadata=self.session.grpc_metadata
                                                                     )
            return dict(response.config_values)
        except Exception as e:
            raise DHError("failed to get the configuration constants.") from e

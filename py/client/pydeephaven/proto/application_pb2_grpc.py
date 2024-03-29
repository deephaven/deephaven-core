# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from pydeephaven.proto import application_pb2 as deephaven_dot_proto_dot_application__pb2


class ApplicationServiceStub(object):
    """
    Allows clients to list fields that are accessible to them.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ListFields = channel.unary_stream(
                '/io.deephaven.proto.backplane.grpc.ApplicationService/ListFields',
                request_serializer=deephaven_dot_proto_dot_application__pb2.ListFieldsRequest.SerializeToString,
                response_deserializer=deephaven_dot_proto_dot_application__pb2.FieldsChangeUpdate.FromString,
                )


class ApplicationServiceServicer(object):
    """
    Allows clients to list fields that are accessible to them.
    """

    def ListFields(self, request, context):
        """
        Request the list of the fields exposed via the worker.

        - The first received message contains all fields that are currently available
        on the worker. None of these fields will be RemovedFields.
        - Subsequent messages modify the existing state. Fields are identified by
        their ticket and may be replaced or removed.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ApplicationServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'ListFields': grpc.unary_stream_rpc_method_handler(
                    servicer.ListFields,
                    request_deserializer=deephaven_dot_proto_dot_application__pb2.ListFieldsRequest.FromString,
                    response_serializer=deephaven_dot_proto_dot_application__pb2.FieldsChangeUpdate.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'io.deephaven.proto.backplane.grpc.ApplicationService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class ApplicationService(object):
    """
    Allows clients to list fields that are accessible to them.
    """

    @staticmethod
    def ListFields(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/io.deephaven.proto.backplane.grpc.ApplicationService/ListFields',
            deephaven_dot_proto_dot_application__pb2.ListFieldsRequest.SerializeToString,
            deephaven_dot_proto_dot_application__pb2.FieldsChangeUpdate.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

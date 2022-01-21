# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: deephaven/proto/object.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from pydeephaven.proto import ticket_pb2 as deephaven_dot_proto_dot_ticket__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='deephaven/proto/object.proto',
  package='io.deephaven.proto.backplane.grpc',
  syntax='proto3',
  serialized_options=b'H\001P\001',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x1c\x64\x65\x65phaven/proto/object.proto\x12!io.deephaven.proto.backplane.grpc\x1a\x1c\x64\x65\x65phaven/proto/ticket.proto\"W\n\x12\x46\x65tchObjectRequest\x12\x41\n\tsource_id\x18\x01 \x01(\x0b\x32..io.deephaven.proto.backplane.grpc.TypedTicket\"z\n\x13\x46\x65tchObjectResponse\x12\x0c\n\x04type\x18\x01 \x01(\t\x12\x0c\n\x04\x64\x61ta\x18\x02 \x01(\x0c\x12G\n\x0ftyped_export_id\x18\x03 \x03(\x0b\x32..io.deephaven.proto.backplane.grpc.TypedTicket2\x8f\x01\n\rObjectService\x12~\n\x0b\x46\x65tchObject\x12\x35.io.deephaven.proto.backplane.grpc.FetchObjectRequest\x1a\x36.io.deephaven.proto.backplane.grpc.FetchObjectResponse\"\x00\x42\x04H\x01P\x01\x62\x06proto3'
  ,
  dependencies=[deephaven_dot_proto_dot_ticket__pb2.DESCRIPTOR,])




_FETCHOBJECTREQUEST = _descriptor.Descriptor(
  name='FetchObjectRequest',
  full_name='io.deephaven.proto.backplane.grpc.FetchObjectRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='source_id', full_name='io.deephaven.proto.backplane.grpc.FetchObjectRequest.source_id', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=97,
  serialized_end=184,
)


_FETCHOBJECTRESPONSE = _descriptor.Descriptor(
  name='FetchObjectResponse',
  full_name='io.deephaven.proto.backplane.grpc.FetchObjectResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='type', full_name='io.deephaven.proto.backplane.grpc.FetchObjectResponse.type', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='data', full_name='io.deephaven.proto.backplane.grpc.FetchObjectResponse.data', index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='typed_export_id', full_name='io.deephaven.proto.backplane.grpc.FetchObjectResponse.typed_export_id', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=186,
  serialized_end=308,
)

_FETCHOBJECTREQUEST.fields_by_name['source_id'].message_type = deephaven_dot_proto_dot_ticket__pb2._TYPEDTICKET
_FETCHOBJECTRESPONSE.fields_by_name['typed_export_id'].message_type = deephaven_dot_proto_dot_ticket__pb2._TYPEDTICKET
DESCRIPTOR.message_types_by_name['FetchObjectRequest'] = _FETCHOBJECTREQUEST
DESCRIPTOR.message_types_by_name['FetchObjectResponse'] = _FETCHOBJECTRESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

FetchObjectRequest = _reflection.GeneratedProtocolMessageType('FetchObjectRequest', (_message.Message,), {
  'DESCRIPTOR' : _FETCHOBJECTREQUEST,
  '__module__' : 'pydeephaven.proto.object_pb2'
  # @@protoc_insertion_point(class_scope:io.deephaven.proto.backplane.grpc.FetchObjectRequest)
  })
_sym_db.RegisterMessage(FetchObjectRequest)

FetchObjectResponse = _reflection.GeneratedProtocolMessageType('FetchObjectResponse', (_message.Message,), {
  'DESCRIPTOR' : _FETCHOBJECTRESPONSE,
  '__module__' : 'pydeephaven.proto.object_pb2'
  # @@protoc_insertion_point(class_scope:io.deephaven.proto.backplane.grpc.FetchObjectResponse)
  })
_sym_db.RegisterMessage(FetchObjectResponse)


DESCRIPTOR._options = None

_OBJECTSERVICE = _descriptor.ServiceDescriptor(
  name='ObjectService',
  full_name='io.deephaven.proto.backplane.grpc.ObjectService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=311,
  serialized_end=454,
  methods=[
  _descriptor.MethodDescriptor(
    name='FetchObject',
    full_name='io.deephaven.proto.backplane.grpc.ObjectService.FetchObject',
    index=0,
    containing_service=None,
    input_type=_FETCHOBJECTREQUEST,
    output_type=_FETCHOBJECTRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_OBJECTSERVICE)

DESCRIPTOR.services_by_name['ObjectService'] = _OBJECTSERVICE

# @@protoc_insertion_point(module_scope)

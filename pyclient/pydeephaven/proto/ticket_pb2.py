# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: deephaven/proto/ticket.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='deephaven/proto/ticket.proto',
  package='io.deephaven.proto.backplane.grpc',
  syntax='proto3',
  serialized_options=b'H\001P\001',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x1c\x64\x65\x65phaven/proto/ticket.proto\x12!io.deephaven.proto.backplane.grpc\"\x18\n\x06Ticket\x12\x0e\n\x06ticket\x18\x01 \x01(\x0c\x42\x04H\x01P\x01\x62\x06proto3'
)




_TICKET = _descriptor.Descriptor(
  name='Ticket',
  full_name='io.deephaven.proto.backplane.grpc.Ticket',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='ticket', full_name='io.deephaven.proto.backplane.grpc.Ticket.ticket', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
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
  serialized_start=67,
  serialized_end=91,
)

DESCRIPTOR.message_types_by_name['Ticket'] = _TICKET
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Ticket = _reflection.GeneratedProtocolMessageType('Ticket', (_message.Message,), {
  'DESCRIPTOR' : _TICKET,
  '__module__' : 'pydeephaven.proto.ticket_pb2'
  # @@protoc_insertion_point(class_scope:io.deephaven.proto.backplane.grpc.Ticket)
  })
_sym_db.RegisterMessage(Ticket)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)

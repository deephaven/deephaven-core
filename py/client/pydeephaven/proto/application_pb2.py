# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: deephaven/proto/application.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from pydeephaven.proto import ticket_pb2 as deephaven_dot_proto_dot_ticket__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n!deephaven/proto/application.proto\x12!io.deephaven.proto.backplane.grpc\x1a\x1c\x64\x65\x65phaven/proto/ticket.proto"\x13\n\x11ListFieldsRequest"\xd1\x01\n\x12\x46ieldsChangeUpdate\x12=\n\x07\x63reated\x18\x01 \x03(\x0b\x32,.io.deephaven.proto.backplane.grpc.FieldInfo\x12=\n\x07updated\x18\x02 \x03(\x0b\x32,.io.deephaven.proto.backplane.grpc.FieldInfo\x12=\n\x07removed\x18\x03 \x03(\x0b\x32,.io.deephaven.proto.backplane.grpc.FieldInfo"\xb2\x01\n\tFieldInfo\x12\x44\n\x0ctyped_ticket\x18\x01 \x01(\x0b\x32..io.deephaven.proto.backplane.grpc.TypedTicket\x12\x12\n\nfield_name\x18\x02 \x01(\t\x12\x19\n\x11\x66ield_description\x18\x03 \x01(\t\x12\x18\n\x10\x61pplication_name\x18\x04 \x01(\t\x12\x16\n\x0e\x61pplication_id\x18\x05 \x01(\t2\x93\x01\n\x12\x41pplicationService\x12}\n\nListFields\x12\x34.io.deephaven.proto.backplane.grpc.ListFieldsRequest\x1a\x35.io.deephaven.proto.backplane.grpc.FieldsChangeUpdate"\x00\x30\x01\x42GH\x01P\x01ZAgithub.com/deephaven/deephaven-core/go/internal/proto/applicationb\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(
    DESCRIPTOR, "deephaven.proto.application_pb2", globals()
)
if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = (
        b"H\001P\001ZAgithub.com/deephaven/deephaven-core/go/internal/proto/application"
    )
    _LISTFIELDSREQUEST._serialized_start = 102
    _LISTFIELDSREQUEST._serialized_end = 121
    _FIELDSCHANGEUPDATE._serialized_start = 124
    _FIELDSCHANGEUPDATE._serialized_end = 333
    _FIELDINFO._serialized_start = 336
    _FIELDINFO._serialized_end = 514
    _APPLICATIONSERVICE._serialized_start = 517
    _APPLICATIONSERVICE._serialized_end = 664
# @@protoc_insertion_point(module_scope)

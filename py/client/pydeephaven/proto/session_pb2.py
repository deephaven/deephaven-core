# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: deephaven/proto/session.proto
# Protobuf Python Version: 5.26.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from pydeephaven.proto import ticket_pb2 as deephaven_dot_proto_dot_ticket__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1d\x64\x65\x65phaven/proto/session.proto\x12!io.deephaven.proto.backplane.grpc\x1a\x1c\x64\x65\x65phaven/proto/ticket.proto\"I\n\x1cWrappedAuthenticationRequest\x12\x0c\n\x04type\x18\x04 \x01(\t\x12\x0f\n\x07payload\x18\x05 \x01(\x0cJ\x04\x08\x02\x10\x03J\x04\x08\x03\x10\x04\"B\n\x10HandshakeRequest\x12\x19\n\rauth_protocol\x18\x01 \x01(\x11\x42\x02\x18\x01\x12\x13\n\x07payload\x18\x02 \x01(\x0c\x42\x02\x18\x01\"\xa2\x01\n\x11HandshakeResponse\x12\x1b\n\x0fmetadata_header\x18\x01 \x01(\x0c\x42\x02\x18\x01\x12\x19\n\rsession_token\x18\x02 \x01(\x0c\x42\x02\x18\x01\x12(\n\x1atoken_deadline_time_millis\x18\x03 \x01(\x12\x42\x04\x18\x01\x30\x01\x12+\n\x1dtoken_expiration_delay_millis\x18\x04 \x01(\x12\x42\x04\x18\x01\x30\x01\"\x16\n\x14\x43loseSessionResponse\"G\n\x0eReleaseRequest\x12\x35\n\x02id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\"\x11\n\x0fReleaseResponse\"\x8b\x01\n\rExportRequest\x12<\n\tsource_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12<\n\tresult_id\x18\x02 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\"\x10\n\x0e\x45xportResponse\"\x8c\x01\n\x0ePublishRequest\x12<\n\tsource_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12<\n\tresult_id\x18\x02 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\"\x11\n\x0fPublishResponse\"\x1b\n\x19\x45xportNotificationRequest\"\xb7\x03\n\x12\x45xportNotification\x12\x39\n\x06ticket\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12Q\n\x0c\x65xport_state\x18\x02 \x01(\x0e\x32;.io.deephaven.proto.backplane.grpc.ExportNotification.State\x12\x0f\n\x07\x63ontext\x18\x03 \x01(\t\x12\x18\n\x10\x64\x65pendent_handle\x18\x04 \x01(\t\"\xe7\x01\n\x05State\x12\x0b\n\x07UNKNOWN\x10\x00\x12\x0b\n\x07PENDING\x10\x01\x12\x0e\n\nPUBLISHING\x10\x02\x12\n\n\x06QUEUED\x10\x03\x12\x0b\n\x07RUNNING\x10\x04\x12\x0c\n\x08\x45XPORTED\x10\x05\x12\x0c\n\x08RELEASED\x10\x06\x12\r\n\tCANCELLED\x10\x07\x12\n\n\x06\x46\x41ILED\x10\x08\x12\x15\n\x11\x44\x45PENDENCY_FAILED\x10\t\x12\x1a\n\x16\x44\x45PENDENCY_NEVER_FOUND\x10\n\x12\x18\n\x14\x44\x45PENDENCY_CANCELLED\x10\x0b\x12\x17\n\x13\x44\x45PENDENCY_RELEASED\x10\x0c\" \n\x1eTerminationNotificationRequest\"\x97\x02\n\x1fTerminationNotificationResponse\x12\x1c\n\x14\x61\x62normal_termination\x18\x01 \x01(\x08\x12\x0e\n\x06reason\x18\x02 \x01(\t\x12\"\n\x1ais_from_uncaught_exception\x18\x03 \x01(\x08\x12\x63\n\x0cstack_traces\x18\x04 \x03(\x0b\x32M.io.deephaven.proto.backplane.grpc.TerminationNotificationResponse.StackTrace\x1a=\n\nStackTrace\x12\x0c\n\x04type\x18\x01 \x01(\t\x12\x0f\n\x07message\x18\x02 \x01(\t\x12\x10\n\x08\x65lements\x18\x03 \x03(\t2\xb9\x08\n\x0eSessionService\x12|\n\nNewSession\x12\x33.io.deephaven.proto.backplane.grpc.HandshakeRequest\x1a\x34.io.deephaven.proto.backplane.grpc.HandshakeResponse\"\x03\x88\x02\x01\x12\x85\x01\n\x13RefreshSessionToken\x12\x33.io.deephaven.proto.backplane.grpc.HandshakeRequest\x1a\x34.io.deephaven.proto.backplane.grpc.HandshakeResponse\"\x03\x88\x02\x01\x12~\n\x0c\x43loseSession\x12\x33.io.deephaven.proto.backplane.grpc.HandshakeRequest\x1a\x37.io.deephaven.proto.backplane.grpc.CloseSessionResponse\"\x00\x12r\n\x07Release\x12\x31.io.deephaven.proto.backplane.grpc.ReleaseRequest\x1a\x32.io.deephaven.proto.backplane.grpc.ReleaseResponse\"\x00\x12y\n\x10\x45xportFromTicket\x12\x30.io.deephaven.proto.backplane.grpc.ExportRequest\x1a\x31.io.deephaven.proto.backplane.grpc.ExportResponse\"\x00\x12|\n\x11PublishFromTicket\x12\x31.io.deephaven.proto.backplane.grpc.PublishRequest\x1a\x32.io.deephaven.proto.backplane.grpc.PublishResponse\"\x00\x12\x8e\x01\n\x13\x45xportNotifications\x12<.io.deephaven.proto.backplane.grpc.ExportNotificationRequest\x1a\x35.io.deephaven.proto.backplane.grpc.ExportNotification\"\x00\x30\x01\x12\xa2\x01\n\x17TerminationNotification\x12\x41.io.deephaven.proto.backplane.grpc.TerminationNotificationRequest\x1a\x42.io.deephaven.proto.backplane.grpc.TerminationNotificationResponse\"\x00\x42\x43H\x01P\x01Z=github.com/deephaven/deephaven-core/go/internal/proto/sessionb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'deephaven.proto.session_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'H\001P\001Z=github.com/deephaven/deephaven-core/go/internal/proto/session'
  _globals['_HANDSHAKEREQUEST'].fields_by_name['auth_protocol']._loaded_options = None
  _globals['_HANDSHAKEREQUEST'].fields_by_name['auth_protocol']._serialized_options = b'\030\001'
  _globals['_HANDSHAKEREQUEST'].fields_by_name['payload']._loaded_options = None
  _globals['_HANDSHAKEREQUEST'].fields_by_name['payload']._serialized_options = b'\030\001'
  _globals['_HANDSHAKERESPONSE'].fields_by_name['metadata_header']._loaded_options = None
  _globals['_HANDSHAKERESPONSE'].fields_by_name['metadata_header']._serialized_options = b'\030\001'
  _globals['_HANDSHAKERESPONSE'].fields_by_name['session_token']._loaded_options = None
  _globals['_HANDSHAKERESPONSE'].fields_by_name['session_token']._serialized_options = b'\030\001'
  _globals['_HANDSHAKERESPONSE'].fields_by_name['token_deadline_time_millis']._loaded_options = None
  _globals['_HANDSHAKERESPONSE'].fields_by_name['token_deadline_time_millis']._serialized_options = b'\030\0010\001'
  _globals['_HANDSHAKERESPONSE'].fields_by_name['token_expiration_delay_millis']._loaded_options = None
  _globals['_HANDSHAKERESPONSE'].fields_by_name['token_expiration_delay_millis']._serialized_options = b'\030\0010\001'
  _globals['_SESSIONSERVICE'].methods_by_name['NewSession']._loaded_options = None
  _globals['_SESSIONSERVICE'].methods_by_name['NewSession']._serialized_options = b'\210\002\001'
  _globals['_SESSIONSERVICE'].methods_by_name['RefreshSessionToken']._loaded_options = None
  _globals['_SESSIONSERVICE'].methods_by_name['RefreshSessionToken']._serialized_options = b'\210\002\001'
  _globals['_WRAPPEDAUTHENTICATIONREQUEST']._serialized_start=98
  _globals['_WRAPPEDAUTHENTICATIONREQUEST']._serialized_end=171
  _globals['_HANDSHAKEREQUEST']._serialized_start=173
  _globals['_HANDSHAKEREQUEST']._serialized_end=239
  _globals['_HANDSHAKERESPONSE']._serialized_start=242
  _globals['_HANDSHAKERESPONSE']._serialized_end=404
  _globals['_CLOSESESSIONRESPONSE']._serialized_start=406
  _globals['_CLOSESESSIONRESPONSE']._serialized_end=428
  _globals['_RELEASEREQUEST']._serialized_start=430
  _globals['_RELEASEREQUEST']._serialized_end=501
  _globals['_RELEASERESPONSE']._serialized_start=503
  _globals['_RELEASERESPONSE']._serialized_end=520
  _globals['_EXPORTREQUEST']._serialized_start=523
  _globals['_EXPORTREQUEST']._serialized_end=662
  _globals['_EXPORTRESPONSE']._serialized_start=664
  _globals['_EXPORTRESPONSE']._serialized_end=680
  _globals['_PUBLISHREQUEST']._serialized_start=683
  _globals['_PUBLISHREQUEST']._serialized_end=823
  _globals['_PUBLISHRESPONSE']._serialized_start=825
  _globals['_PUBLISHRESPONSE']._serialized_end=842
  _globals['_EXPORTNOTIFICATIONREQUEST']._serialized_start=844
  _globals['_EXPORTNOTIFICATIONREQUEST']._serialized_end=871
  _globals['_EXPORTNOTIFICATION']._serialized_start=874
  _globals['_EXPORTNOTIFICATION']._serialized_end=1313
  _globals['_EXPORTNOTIFICATION_STATE']._serialized_start=1082
  _globals['_EXPORTNOTIFICATION_STATE']._serialized_end=1313
  _globals['_TERMINATIONNOTIFICATIONREQUEST']._serialized_start=1315
  _globals['_TERMINATIONNOTIFICATIONREQUEST']._serialized_end=1347
  _globals['_TERMINATIONNOTIFICATIONRESPONSE']._serialized_start=1350
  _globals['_TERMINATIONNOTIFICATIONRESPONSE']._serialized_end=1629
  _globals['_TERMINATIONNOTIFICATIONRESPONSE_STACKTRACE']._serialized_start=1568
  _globals['_TERMINATIONNOTIFICATIONRESPONSE_STACKTRACE']._serialized_end=1629
  _globals['_SESSIONSERVICE']._serialized_start=1632
  _globals['_SESSIONSERVICE']._serialized_end=2713
# @@protoc_insertion_point(module_scope)

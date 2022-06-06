# This should be removed later in favor of a gradle build process

protoc --go_out=. --go_opt=module="github.com/deephaven/deephaven-core/go-client" --go-grpc_out=. --go-grpc_opt=module="github.com/deephaven/deephaven-core/go-client" \
	--experimental_allow_proto3_optional	\
	-I../proto/proto-backplane-grpc/src/main/proto	\
	deephaven/proto/application.proto	\
	deephaven/proto/console.proto		\
	deephaven/proto/inputtable.proto	\
	deephaven/proto/object.proto		\
	deephaven/proto/session.proto		\
	deephaven/proto/table.proto			\
	deephaven/proto/ticket.proto

	#--go_opt=Mdeephaven/proto/application.proto=github.com/deephaven/deephaven-core/dh_client/proto/application_pb2	\
	#--go_opt=Mdeephaven/proto/console.proto=github.com/deephaven/deephaven-core/dh_client/proto/console_pb2	\
	#--go_opt=Mdeephaven/proto/inputtable.proto=github.com/deephaven/deephaven-core/dh_client/proto/inputtable_pb2	\
	#--go_opt=Mdeephaven/proto/object.proto=github.com/deephaven/deephaven-core/dh_client/proto/object_pb2	\
	#--go_opt=Mdeephaven/proto/session.proto=github.com/deephaven-deephaven-core/dh_client/proto/session_pb2	\
	#--go_opt=Mdeephaven/proto/table.proto=github.com/deephaven/deephaven-core/dh_client/proto/table_pb2	\
	#--go_opt=Mdeephaven/proto/ticket.proto=github.com/deephaven/deephaven-core/dh_client/proto/ticket_pb2	\

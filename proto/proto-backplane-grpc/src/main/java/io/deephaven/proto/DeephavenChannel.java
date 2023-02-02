package io.deephaven.proto;

import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceStub;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc.InputTableServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc.InputTableServiceFutureStub;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc.InputTableServiceStub;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc.ObjectServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc.ObjectServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc.ObjectServiceStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceFutureStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceFutureStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceBlockingStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceFutureStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;

public interface DeephavenChannel {
    SessionServiceStub session();

    TableServiceStub table();

    ConsoleServiceStub console();

    ObjectServiceStub object();

    ApplicationServiceStub application();

    InputTableServiceStub inputTable();

    SessionServiceBlockingStub sessionBlocking();

    TableServiceBlockingStub tableBlocking();

    ConsoleServiceBlockingStub consoleBlocking();

    ObjectServiceBlockingStub objectBlocking();

    ApplicationServiceBlockingStub applicationBlocking();

    InputTableServiceBlockingStub inputTableBlocking();

    SessionServiceFutureStub sessionFuture();

    TableServiceFutureStub tableFuture();

    ConsoleServiceFutureStub consoleFuture();

    ObjectServiceFutureStub objectFuture();

    ApplicationServiceFutureStub applicationFuture();

    InputTableServiceFutureStub inputTableFuture();
}

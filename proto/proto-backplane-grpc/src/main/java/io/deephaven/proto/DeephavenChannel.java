//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto;

import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceStub;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc.ConfigServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc.ConfigServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc.ConfigServiceStub;
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
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;

public interface DeephavenChannel {
    static DeephavenChannel withCallCredentials(DeephavenChannel channel, CallCredentials callCredentials) {
        return new DeephavenChannelWithCallCredentials(channel, callCredentials);
    }

    static DeephavenChannel withClientInterceptors(DeephavenChannel channel, ClientInterceptor... clientInterceptors) {
        return new DeephavenChannelWithClientInterceptors(channel, clientInterceptors);
    }

    Channel channel();

    SessionServiceStub session();

    TableServiceStub table();

    ConsoleServiceStub console();

    ObjectServiceStub object();

    ApplicationServiceStub application();

    InputTableServiceStub inputTable();

    ConfigServiceStub config();

    SessionServiceBlockingStub sessionBlocking();

    TableServiceBlockingStub tableBlocking();

    ConsoleServiceBlockingStub consoleBlocking();

    ObjectServiceBlockingStub objectBlocking();

    ApplicationServiceBlockingStub applicationBlocking();

    InputTableServiceBlockingStub inputTableBlocking();

    ConfigServiceBlockingStub configBlocking();

    SessionServiceFutureStub sessionFuture();

    TableServiceFutureStub tableFuture();

    ConsoleServiceFutureStub consoleFuture();

    ObjectServiceFutureStub objectFuture();

    ApplicationServiceFutureStub applicationFuture();

    InputTableServiceFutureStub inputTableFuture();

    ConfigServiceFutureStub configFuture();
}

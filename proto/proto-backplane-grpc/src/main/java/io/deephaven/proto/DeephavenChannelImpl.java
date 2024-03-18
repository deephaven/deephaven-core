//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto;

import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceStub;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc.ConfigServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc.ConfigServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc.ConfigServiceStub;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc.InputTableServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc.InputTableServiceFutureStub;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc.InputTableServiceStub;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc.ObjectServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc.ObjectServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc.ObjectServiceStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceFutureStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceFutureStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceBlockingStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceFutureStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;
import io.grpc.Channel;

import javax.inject.Inject;
import java.util.Objects;

/**
 * A Deephaven service helper for a {@link Channel channel}.
 */
public final class DeephavenChannelImpl implements DeephavenChannel {
    private final Channel channel;

    @Inject
    public DeephavenChannelImpl(Channel channel) {
        this.channel = Objects.requireNonNull(channel);
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public SessionServiceStub session() {
        return SessionServiceGrpc.newStub(channel);
    }

    @Override
    public TableServiceStub table() {
        return TableServiceGrpc.newStub(channel);
    }

    @Override
    public ConsoleServiceStub console() {
        return ConsoleServiceGrpc.newStub(channel);
    }

    @Override
    public ObjectServiceStub object() {
        return ObjectServiceGrpc.newStub(channel);
    }

    @Override
    public ApplicationServiceStub application() {
        return ApplicationServiceGrpc.newStub(channel);
    }

    @Override
    public InputTableServiceStub inputTable() {
        return InputTableServiceGrpc.newStub(channel);
    }

    @Override
    public ConfigServiceStub config() {
        return ConfigServiceGrpc.newStub(channel);
    }

    @Override
    public SessionServiceBlockingStub sessionBlocking() {
        return SessionServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public TableServiceBlockingStub tableBlocking() {
        return TableServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public ConsoleServiceBlockingStub consoleBlocking() {
        return ConsoleServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public ObjectServiceBlockingStub objectBlocking() {
        return ObjectServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public ApplicationServiceBlockingStub applicationBlocking() {
        return ApplicationServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public InputTableServiceBlockingStub inputTableBlocking() {
        return InputTableServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public ConfigServiceBlockingStub configBlocking() {
        return ConfigServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public SessionServiceFutureStub sessionFuture() {
        return SessionServiceGrpc.newFutureStub(channel);
    }

    @Override
    public TableServiceFutureStub tableFuture() {
        return TableServiceGrpc.newFutureStub(channel);
    }

    @Override
    public ConsoleServiceFutureStub consoleFuture() {
        return ConsoleServiceGrpc.newFutureStub(channel);
    }

    @Override
    public ObjectServiceFutureStub objectFuture() {
        return ObjectServiceGrpc.newFutureStub(channel);
    }

    @Override
    public ApplicationServiceFutureStub applicationFuture() {
        return ApplicationServiceGrpc.newFutureStub(channel);
    }

    @Override
    public InputTableServiceFutureStub inputTableFuture() {
        return InputTableServiceGrpc.newFutureStub(channel);
    }

    @Override
    public ConfigServiceFutureStub configFuture() {
        return ConfigServiceGrpc.newFutureStub(channel);
    }
}

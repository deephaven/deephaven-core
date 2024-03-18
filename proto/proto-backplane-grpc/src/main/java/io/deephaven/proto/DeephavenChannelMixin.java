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
import io.grpc.Channel;
import io.grpc.stub.AbstractStub;

import java.util.Objects;

public abstract class DeephavenChannelMixin implements DeephavenChannel {
    private final DeephavenChannel delegate;

    public DeephavenChannelMixin(DeephavenChannel delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    protected abstract <S extends AbstractStub<S>> S mixin(S stub);

    protected abstract Channel mixinChannel(Channel channel);

    @Override
    public Channel channel() {
        return mixinChannel(delegate.channel());
    }

    @Override
    public final SessionServiceStub session() {
        return mixin(delegate.session());
    }

    @Override
    public final TableServiceStub table() {
        return mixin(delegate.table());
    }

    @Override
    public final ConsoleServiceStub console() {
        return mixin(delegate.console());
    }

    @Override
    public final ObjectServiceStub object() {
        return mixin(delegate.object());
    }

    @Override
    public final ApplicationServiceStub application() {
        return mixin(delegate.application());
    }

    @Override
    public final InputTableServiceStub inputTable() {
        return mixin(delegate.inputTable());
    }

    @Override
    public ConfigServiceStub config() {
        return mixin(delegate.config());
    }

    @Override
    public final SessionServiceBlockingStub sessionBlocking() {
        return mixin(delegate.sessionBlocking());
    }

    @Override
    public final TableServiceBlockingStub tableBlocking() {
        return mixin(delegate.tableBlocking());
    }

    @Override
    public final ConsoleServiceBlockingStub consoleBlocking() {
        return mixin(delegate.consoleBlocking());
    }

    @Override
    public final ObjectServiceBlockingStub objectBlocking() {
        return mixin(delegate.objectBlocking());
    }

    @Override
    public final ApplicationServiceBlockingStub applicationBlocking() {
        return mixin(delegate.applicationBlocking());
    }

    @Override
    public final InputTableServiceBlockingStub inputTableBlocking() {
        return mixin(delegate.inputTableBlocking());
    }

    @Override
    public ConfigServiceBlockingStub configBlocking() {
        return mixin(delegate.configBlocking());
    }

    @Override
    public final SessionServiceFutureStub sessionFuture() {
        return mixin(delegate.sessionFuture());
    }

    @Override
    public final TableServiceFutureStub tableFuture() {
        return mixin(delegate.tableFuture());
    }

    @Override
    public final ConsoleServiceFutureStub consoleFuture() {
        return mixin(delegate.consoleFuture());
    }

    @Override
    public final ObjectServiceFutureStub objectFuture() {
        return mixin(delegate.objectFuture());
    }

    @Override
    public final ApplicationServiceFutureStub applicationFuture() {
        return mixin(delegate.applicationFuture());
    }

    @Override
    public final InputTableServiceFutureStub inputTableFuture() {
        return mixin(delegate.inputTableFuture());
    }

    @Override
    public ConfigServiceFutureStub configFuture() {
        return mixin(delegate.configFuture());
    }
}

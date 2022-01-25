package io.deephaven.proto;

import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceStub;
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
public class DeephavenChannel {
    private final Channel channel;

    @Inject
    public DeephavenChannel(Channel channel) {
        this.channel = Objects.requireNonNull(channel);
    }

    public Channel channel() {
        return channel;
    }

    public SessionServiceStub session() {
        return SessionServiceGrpc.newStub(channel);
    }

    public TableServiceStub table() {
        return TableServiceGrpc.newStub(channel);
    }

    public ConsoleServiceStub console() {
        return ConsoleServiceGrpc.newStub(channel);
    }

    public ObjectServiceStub object() {
        return ObjectServiceGrpc.newStub(channel);
    }

    public ApplicationServiceStub application() {
        return ApplicationServiceGrpc.newStub(channel);
    }

    public InputTableServiceStub inputTable() {
        return InputTableServiceGrpc.newStub(channel);
    }

    public SessionServiceBlockingStub sessionBlocking() {
        return SessionServiceGrpc.newBlockingStub(channel);
    }

    public TableServiceBlockingStub tableBlocking() {
        return TableServiceGrpc.newBlockingStub(channel);
    }

    public ConsoleServiceBlockingStub consoleBlocking() {
        return ConsoleServiceGrpc.newBlockingStub(channel);
    }

    public ObjectServiceBlockingStub objectBlocking() {
        return ObjectServiceGrpc.newBlockingStub(channel);
    }

    public ApplicationServiceBlockingStub applicationBlocking() {
        return ApplicationServiceGrpc.newBlockingStub(channel);
    }

    public InputTableServiceBlockingStub inputTableBlocking() {
        return InputTableServiceGrpc.newBlockingStub(channel);
    }

    public SessionServiceFutureStub sessionFuture() {
        return SessionServiceGrpc.newFutureStub(channel);
    }

    public TableServiceFutureStub tableFuture() {
        return TableServiceGrpc.newFutureStub(channel);
    }

    public ConsoleServiceFutureStub consoleFuture() {
        return ConsoleServiceGrpc.newFutureStub(channel);
    }

    public ObjectServiceFutureStub objectFuture() {
        return ObjectServiceGrpc.newFutureStub(channel);
    }

    public ApplicationServiceFutureStub applicationFuture() {
        return ApplicationServiceGrpc.newFutureStub(channel);
    }

    public InputTableServiceFutureStub inputTableFuture() {
        return InputTableServiceGrpc.newFutureStub(channel);
    }
}

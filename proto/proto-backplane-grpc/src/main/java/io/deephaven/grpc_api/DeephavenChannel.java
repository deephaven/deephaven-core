package io.deephaven.grpc_api;

import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceStub;
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
import io.grpc.ManagedChannel;

import javax.inject.Inject;
import java.util.Objects;

/**
 * A Deephaven service helper for a {@link ManagedChannel managed channel}.
 */
public class DeephavenChannel {
    private final ManagedChannel channel;

    @Inject
    public DeephavenChannel(ManagedChannel channel) {
        this.channel = Objects.requireNonNull(channel);
    }

    public ManagedChannel channel() {
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

    public ApplicationServiceStub application() {
        return ApplicationServiceGrpc.newStub(channel);
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

    public ApplicationServiceBlockingStub applicationBlocking() {
        return ApplicationServiceGrpc.newBlockingStub(channel);
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

    public ApplicationServiceFutureStub applicationFuture() {
        return ApplicationServiceGrpc.newFutureStub(channel);
    }
}

package io.deephaven.client;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.client.impl.SessionImplConfig;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;
import io.grpc.ManagedChannel;

import javax.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Module
public interface SessionImplModule {

    @Provides
    static SessionImpl session(ManagedChannel managedChannel, ScheduledExecutorService scheduler) {
        return SessionImplConfig.builder()
            .executor(scheduler)
            .tableService(TableServiceGrpc.newStub(managedChannel))
            .sessionService(SessionServiceGrpc.newStub(managedChannel))
            .consoleService(ConsoleServiceGrpc.newStub(managedChannel))
            .build()
            .createSession(SessionServiceGrpc.newBlockingStub(managedChannel));
    }

    @Provides
    static CompletableFuture<? extends SessionImpl> sessionFuture(ManagedChannel managedChannel,
        ScheduledExecutorService scheduler) {
        return SessionImplConfig.builder()
            .executor(scheduler)
            .tableService(TableServiceGrpc.newStub(managedChannel))
            .sessionService(SessionServiceGrpc.newStub(managedChannel))
            .consoleService(ConsoleServiceGrpc.newStub(managedChannel))
            .build()
            .createSessionFuture();
    }
}

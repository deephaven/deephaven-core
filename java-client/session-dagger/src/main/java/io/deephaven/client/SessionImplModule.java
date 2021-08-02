package io.deephaven.client;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Module
public interface SessionImplModule {

    @Provides
    static SessionImpl providesSession(SessionServiceBlockingStub stubBlocking,
        SessionServiceStub stub, TableServiceStub tableServiceStub,
        ConsoleServiceStub consoleServiceStub, ScheduledExecutorService executor) {
        return SessionImpl.create(stubBlocking, stub, tableServiceStub, consoleServiceStub,
            executor);
    }

    @Provides
    static CompletableFuture<? extends SessionImpl> providesSessionFuture(SessionServiceStub stub,
        TableServiceStub tableServiceStub, ConsoleServiceStub consoleServiceStub,
        ScheduledExecutorService executor) {
        return SessionImpl.create(stub, tableServiceStub, consoleServiceStub, executor);
    }
}

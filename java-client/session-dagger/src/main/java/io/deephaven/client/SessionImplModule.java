package io.deephaven.client;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;

import javax.inject.Named;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Module
public interface SessionImplModule {

    @Provides
    static SessionImpl providesSession(SessionServiceBlockingStub stubBlocking,
        SessionServiceStub stub, TableServiceStub tableServiceStub,
        ConsoleServiceStub consoleServiceStub, ScheduledExecutorService executor,
        @Named("delegateToBatch") boolean delegateToBatch) {
        return SessionImpl.create(stubBlocking, stub, tableServiceStub, consoleServiceStub,
            executor, delegateToBatch);
    }

    @Provides
    static CompletableFuture<? extends SessionImpl> providesSessionFuture(SessionServiceStub stub,
        TableServiceStub tableServiceStub, ConsoleServiceStub consoleServiceStub,
        ScheduledExecutorService executor, @Named("delegateToBatch") boolean delegateToBatch) {
        return SessionImpl.create(stub, tableServiceStub, consoleServiceStub, executor,
            delegateToBatch);
    }

    @Provides
    @Named("delegateToBatch")
    static boolean providesDelegateToBatch() {
        return Boolean.getBoolean("io.deephaven.client.impl.SessionImpl.delegateToBatch");
    }
}

package io.deephaven.client;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.client.impl.SessionImplConfig;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Module
public interface SessionImplModule {

    @Provides
    static SessionImplConfig providesSessionImplConfig(ScheduledExecutorService executor,
        SessionServiceStub sessionService, TableServiceStub tableService,
        ConsoleServiceStub consoleService) {
        return SessionImplConfig.builder().executor(executor).sessionService(sessionService)
            .tableService(tableService).consoleService(consoleService).build();
    }

    @Provides
    static CompletableFuture<? extends SessionImpl> providesSessionFuture(
        SessionImplConfig config) {
        return config.createSessionFuture();
    }

    @Provides
    static SessionImpl providesSession(SessionImplConfig config,
        SessionServiceBlockingStub stubBlocking) {
        return config.createSession(stubBlocking);
    }
}

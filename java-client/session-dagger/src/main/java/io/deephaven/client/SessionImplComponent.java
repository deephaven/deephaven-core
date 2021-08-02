package io.deephaven.client;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.client.impl.SessionFactory;
import io.deephaven.client.impl.SessionImpl;
import io.grpc.ManagedChannel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Component(modules = {SessionImplModule.class, ServicesModule.class, GrpcModule.class})
public interface SessionImplComponent extends SessionFactory {

    SessionImpl session();

    CompletableFuture<? extends SessionImpl> sessionFuture();

    @Component.Factory
    interface Factory {
        SessionImplComponent create(@BindsInstance ManagedChannel managedChannel,
            @BindsInstance ScheduledExecutorService scheduler);
    }
}

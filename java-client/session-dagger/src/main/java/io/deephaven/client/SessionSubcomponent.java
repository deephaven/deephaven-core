package io.deephaven.client;

import dagger.BindsInstance;
import dagger.Module;
import dagger.Subcomponent;
import io.deephaven.client.impl.SessionFactory;
import io.deephaven.client.impl.SessionImpl;
import io.grpc.ManagedChannel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Subcomponent(modules = SessionImplModule.class)
public interface SessionSubcomponent extends SessionFactory {

    SessionImpl newSession();

    CompletableFuture<? extends SessionImpl> newSessionFuture();

    @Module(subcomponents = SessionSubcomponent.class)
    interface SessionFactorySubcomponentModule {

    }

    @Subcomponent.Builder
    interface Builder {
        Builder managedChannel(@BindsInstance ManagedChannel channel);

        Builder scheduler(@BindsInstance ScheduledExecutorService scheduler);

        // TODO(deephaven-core#1157): Plumb SessionImplConfig.Builder options through dagger

        SessionSubcomponent build();
    }
}

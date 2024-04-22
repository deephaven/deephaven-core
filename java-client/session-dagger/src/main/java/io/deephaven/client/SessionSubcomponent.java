//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import dagger.BindsInstance;
import dagger.Module;
import dagger.Subcomponent;
import io.deephaven.client.impl.SessionFactory;
import io.deephaven.client.impl.SessionImpl;
import io.grpc.ManagedChannel;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.util.concurrent.ScheduledExecutorService;

@Subcomponent(modules = SessionImplModule.class)
public interface SessionSubcomponent extends SessionFactory {

    @Override
    SessionImpl newSession();

    @Override
    ManagedChannel managedChannel();

    @Module(subcomponents = SessionSubcomponent.class)
    interface SessionFactorySubcomponentModule {

    }

    @Subcomponent.Builder
    interface Builder {
        Builder managedChannel(@BindsInstance ManagedChannel channel);

        Builder scheduler(@BindsInstance ScheduledExecutorService scheduler);

        Builder authenticationTypeAndValue(
                @BindsInstance @Nullable @Named("authenticationTypeAndValue") String authenticationTypeAndValue);

        // TODO(deephaven-core#1157): Plumb SessionImplConfig.Builder options through dagger

        SessionSubcomponent build();
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import dagger.BindsInstance;
import dagger.Module;
import dagger.Subcomponent;
import io.deephaven.client.SessionImplModule;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The barrage subcomponent.
 *
 * @see SessionImplModule
 * @see FlightSessionModule
 * @see BarrageSessionModule
 */
@Subcomponent(modules = {SessionImplModule.class, FlightSessionModule.class, BarrageSessionModule.class})
public interface BarrageSubcomponent extends BarrageSessionFactory {

    @Override
    BarrageSession newBarrageSession();

    @Override
    ManagedChannel managedChannel();

    @Module(subcomponents = {BarrageSubcomponent.class})
    interface DeephavenClientSubcomponentModule {

    }

    @Subcomponent.Builder
    interface Builder extends BarrageSessionFactoryBuilder {
        Builder managedChannel(@BindsInstance ManagedChannel channel);

        Builder scheduler(@BindsInstance ScheduledExecutorService scheduler);

        Builder allocator(@BindsInstance BufferAllocator bufferAllocator);

        Builder authenticationTypeAndValue(
                @BindsInstance @Nullable @Named("authenticationTypeAndValue") String authenticationTypeAndValue);

        BarrageSubcomponent build();
    }
}

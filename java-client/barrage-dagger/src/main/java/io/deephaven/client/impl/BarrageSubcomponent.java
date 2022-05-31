/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import dagger.BindsInstance;
import dagger.Module;
import dagger.Subcomponent;
import io.deephaven.client.SessionImplModule;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Subcomponent(modules = {SessionImplModule.class, FlightSessionModule.class, BarrageSessionModule.class})
public interface BarrageSubcomponent extends BarrageSessionFactory {

    BarrageSession newBarrageSession();

    CompletableFuture<? extends BarrageSession> newBarrageSessionFuture();

    @Module(subcomponents = {BarrageSubcomponent.class})
    interface DeephavenClientSubcomponentModule {

    }

    @Subcomponent.Builder
    interface Builder extends BarrageSessionFactoryBuilder {
        Builder managedChannel(@BindsInstance ManagedChannel channel);

        Builder scheduler(@BindsInstance ScheduledExecutorService scheduler);

        Builder allocator(@BindsInstance BufferAllocator bufferAllocator);

        BarrageSubcomponent build();
    }
}

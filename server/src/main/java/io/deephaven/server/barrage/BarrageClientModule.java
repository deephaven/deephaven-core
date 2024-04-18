//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.client.impl.BarrageFactoryBuilderModule;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import javax.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides a singleton {@link BufferAllocator}, a singleton {@link ScheduledExecutorService}, and binds
 * {@link BarrageSessionFactoryClient.Application}.
 */
@Module(includes = BarrageFactoryBuilderModule.class)
public interface BarrageClientModule {

    /**
     * Equivalent to {@code new RootAllocator()}.
     *
     * @see RootAllocator
     */
    @Provides
    @Singleton
    static BufferAllocator providesAllocator() {
        return new RootAllocator();
    }

    /**
     * Equivalent to {@code Executors.newScheduledThreadPool(4)}.
     *
     * @see Executors#newScheduledThreadPool(int)
     */
    @Provides
    @Singleton
    static ScheduledExecutorService providesScheduler() {
        return Executors.newScheduledThreadPool(4);
    }

    /**
     * Binds {@link BarrageSessionFactoryClient.Application}.
     */
    @Binds
    @IntoSet
    ApplicationState.Factory bindBarrageSessionFactoryClientApplication(
            BarrageSessionFactoryClient.Application application);
}

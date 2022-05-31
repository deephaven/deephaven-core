package io.deephaven.server.barrage;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.BarrageFactoryBuilderModule;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import javax.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Module(includes = BarrageFactoryBuilderModule.class)
public interface BarrageClientModule {

    @Provides
    @Singleton
    static BufferAllocator providesAllocator() {
        return new RootAllocator();
    }

    @Provides
    @Singleton
    static ScheduledExecutorService providesScheduler() {
        return Executors.newScheduledThreadPool(4);
    }
}

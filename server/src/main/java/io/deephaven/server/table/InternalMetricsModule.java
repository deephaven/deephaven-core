package io.deephaven.server.table;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.table.impl.util.MemoryTableLogger;
import io.deephaven.engine.table.impl.util.InternalTableLoggerWrapper;

@Module
public interface InternalMetricsModule {
    @Provides
    static InternalTableLoggerWrapper.Factory providesTableLoggerWrapperFactory() {
        return MemoryTableLogger::new;
    }
}
